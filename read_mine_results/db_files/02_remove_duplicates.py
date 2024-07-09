# Description:
# Rename the names of the compounds and reactions, so they are unique and differantiable to the other tables.
# The main reason is, because pickaxe will name the pkc and pkr just from 1 to ...


# time to run for 3 files:
# real    6m6.483s
# user    9m32.428s
# sys     1m7.996s

# files for renaming should be in one folder (renamed files will be recognized)
path_to_files = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/renamed_files/"
path_to_save = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/removed_duplicates/"

import polars as pl
import os
import re
import sys
import re

pl.Config(fmt_str_lengths=550)
pl.Config.set_tbl_rows(100)





def lazyread_mines_parquet(parquet_file):
    """
    Try to read in a parquet file with the filename included "compounds" or "reactions".

    Args:
        parquet_file: path to parquet file.

    Returns:
        returns the lazyframe for polars

    Improvments:
        - if path is longer, it should only check for the filnema if compounds or reaction...
    """

    # all(substring in item for item in lst)
    # in the moment doesn't works for lists
    if "compounds" in parquet_file:
        print(f'Compound file - filter for predicted compounds - read in : {parquet_file}')
        lazy_df = (
            pl.scan_parquet(parquet_file)
            .filter(pl.col("Type") == "Predicted")   
            )
    elif "reactions" in parquet_file:
        print(f'Reaction file read in : {parquet_file}')
        lazy_df = (
            pl.scan_parquet(parquet_file)
            .drop("Name")
            )
    else:
        print(f'other file - read in :\n {parquet_file}')
        lazy_df = (
            pl.scan_parquet(parquet_file)
            )
    return lazy_df


def get_files_in_folder(directory):
    # List to store full paths of files
    file_paths = []

    # List all entries in the directory
    for entry in os.listdir(directory):
        # Get the full path by joining directory and entry
        full_path = os.path.join(directory, entry)
        
        # Check if it's a file
        if os.path.isfile(full_path):
            file_paths.append(full_path)

    return file_paths


def find_two_chars_after_word(text, word):
    # Escape the word to handle any special characters in the word
    escaped_word = re.escape(word)
    
    # Use a regular expression to count occurrences of the word
    occurrences = len(re.findall(escaped_word, text))
    
    if occurrences == 1:
        # Use a regular expression to find the word followed by two characters
        pattern = rf'{escaped_word}(.{{2}})'
        match = re.search(pattern, text)
        
        if match:
            return match.group(1)
        else:
            return None
    else:
        return None


def reorder(row):
        col1_value, col2_value = row[0], row[1]
        col1_suffix = int(col1_value.split('_')[1])
        # col2_suffix = int(col2_value.split('_')[1])
        
        if col1_suffix == "02":
            return col1_value
        else:
            return col2_value



def get_merged_compoundfile(compoundfile, merged_in_compoundfile, equal_columns = ['Formula', 'InChIKey', 'SMILES'], info = False):
    # read both files at once
    lf_compound = lazyread_mines_parquet([compoundfile, merged_in_compoundfile])

    # duplicate is not in lazyframe (collecting step should be in the end if possible)
    df_compounds = lf_compound.select(['ID', 'Formula', 'InChIKey', 'SMILES']).collect(streaming = True)
    
    # find duplicates in compounds
    duplicated_compounds = df_compounds.filter(pl.lit(df_compounds.select(equal_columns).is_duplicated())).sort(equal_columns)

    # we don't want multiple replicates (like 3x...)
    check_if_only_duplicates = lf_compound.group_by(equal_columns).agg(pl.len().alias("count")).filter(pl.col("count") > 1).select("count").unique().collect().item()
            
    if (check_if_only_duplicates == 2):
        # Extract the first and the second values from duplicates

        # Add an index column
        duplicated_compounds = duplicated_compounds.with_row_index("index")

        # Filter for odd indices
        odd_indices_df = duplicated_compounds.filter(pl.col("index") % 2 != 0).select(pl.col("ID").alias("compound_1"))

        # Filter for even indices
        even_indices_df = duplicated_compounds.filter(pl.col("index") % 2 == 0).select(pl.col("ID").alias("compound_2"))

        # Combine the two DataFrames
        combined_df = pl.concat([odd_indices_df, even_indices_df], how="horizontal")

        # print("dataframe to tranform at list:", combined_df)

        # reorder the files        
        pattern_two_characters_after_split = rf"_split_(.{{{2}}})"
        filenumbers_compound_1 = re.search(pattern_two_characters_after_split, compoundfile)
        if filenumbers_compound_1:
            filenumber_compound_1 = filenumbers_compound_1.group(1)
            print(f"The characters following '{compoundfile}' we found: '{filenumber_compound_1}'")
        else:
            print(f"The word '{result}' was not found in the text.")

        # print(f"original: {filenumbers_compound_1}")

        condition_wrong_sorted = (
            pl.col("compound_2").str.ends_with(filenumber_compound_1)
        )

        # Apply the filter condition 
        compounds_to_rename = combined_df.filter(condition_wrong_sorted).select([
            pl.col("compound_2").alias("compound_1"),
            pl.col("compound_1").alias("compound_2")
        ])

        compounds_to_not_rename = combined_df.filter(~condition_wrong_sorted)

        duplicated_compounds_df = pl.DataFrame({
            "compound_1": pl.concat([compounds_to_rename.select(["compound_1"]), compounds_to_not_rename.select(["compound_1"])]),
            "compound_2": pl.concat([compounds_to_rename.select(["compound_2"]), compounds_to_not_rename.select(["compound_2"])])
        })        

        # Get even rows
        second_compound = duplicated_compounds_df["compound_2"].to_list() 

        # Get odd rows
        first_compound = duplicated_compounds_df["compound_1"].to_list() 

        # Create a dictionary from the two lists
        compounds_to_rename = dict(zip(first_compound, second_compound))


        # # Get information about the compounds
        # print(compounds_to_rename)
        # print(compounds_to_not_rename)
        # print(duplicated_compounds_df)
        # feedback = list(compounds_to_rename.items())[:15]
        # print(feedback)

    else:
        sys.exit("More duplicates then expected:", check_if_only_duplicates)

    if info:
        all_compounds = df_compounds.shape[0]
        duplicated_compounds = int(duplicated_compounds.shape[0] / 2)
        unique_compounds = lf_compound.select(equal_columns).unique().collect().shape[0]

        filename_1 = compoundfile.split("/")[-1]
        filename_2 = merged_in_compoundfile.split("/")[-1]

        print(f'File 1: {filename_1},\nFile 2: {filename_2}\nTotal: {all_compounds:_d}\nunique compounds: {unique_compounds:_d} ({duplicated_compounds/all_compounds*100:.2f} % - {duplicated_compounds:_d} duplicates)\n-----')

    return first_compound, second_compound #compounds_to_rename


# def rename_compounds(compoundfile, suffix, output_file = ""):
#     lf_compounds = lazyread_mines_parquet(compoundfile)

#     # Append the suffix to all values in the specified column
#     lf_compounds = lf_compounds.with_columns(
#         (pl.col("ID") + suffix).alias("ID")
#     )

#     # Collect the lazy frame to execute the operations
#     df_compounds = lf_compounds.collect()

#     if output_file:
#         df_compounds.write_parquet(output_file)
    
#     return 0


# def rename_reactions(reactionfile, suffix, output_file = ""):

#     # read in reactionfile
#     lf_reactions = lazyread_mines_parquet(reactionfile)

#     # Append the suffix to all values in the column "ID"
#     lf_reactions = lf_reactions.with_columns(
#         (pl.col("ID") + suffix).alias("ID")
#     )

#     # Regex pattern to find 'pkc' followed by digits and '[c0]'
#     pattern = r"(pkc\d+)(\[c0\])" 

#     # Define the replacement expressions for the column "ID equation"
#     replacement_expr_pkc = pl.col("ID equation").str.replace_all(pattern, r"${1}_XXX-pkc-XXX_${2}") 
 
#     replacement_expr_suffix = pl.col("ID equation").str.replace_all(r"_XXX-pkc-XXX_", suffix) 

#     # Apply the replacement expression to the specified column
#     lf_reactions = lf_reactions.with_columns(replacement_expr_pkc.alias("ID equation"))
#     lf_reactions = lf_reactions.with_columns(replacement_expr_suffix.alias("ID equation"))
    
#     df_reactions = lf_reactions.collect()

#     if output_file:
#         df_reactions.write_parquet(output_file)

#     return df_reactions


# get all filenames with path
filenames = get_files_in_folder(path_to_files)

# filter out the compound and reaction files
reaction_files = []
compound_files = []

for file in filenames:

    # get the filename (last element)
    filename = file.split("/")[-1]
    
    # get all the compounds
    if "reactions" in filename:
        reaction_files.append(file)

    # get all the reactions
    if "compounds" in filename:
        compound_files.append(file)

# sort the lists
reaction_files.sort()
compound_files.sort()

compound_files2 = compound_files.copy()

removed_compounds = list()

# go through all the files
for compoundfile1 in compound_files:

    # remove the compound from the list for better performance
    compound_files2.remove(compoundfile1)

    for compoundfile2 in compound_files2:
 
        # compounds_dict_to_rename
        first_compound_duplicates, second_compound_duplicates = get_merged_compoundfile(compoundfile1, compoundfile2, info = True)

        # drop all duplicates from the second file (compoundfile2)
        df_compoundfile2 = lazyread_mines_parquet(compoundfile2)
        df_compoundfile2 = df_compoundfile2.filter(pl.col("ID").is_in(first_compound_duplicates))

        # overwrite the existing file
        df_compoundfile2.sink_parquet(compoundfile2) # + "_removed_duplicates.parquet")

        # find the reactionfile to rename (second file)
        pattern_two_characters_after_split = rf"_split_(.{{{2}}})"

        filenumbers_compound_2 = re.search(pattern_two_characters_after_split, compoundfile2)
        if filenumbers_compound_2:
            filenumbers_compound_2 = filenumbers_compound_2.group(1)
            print(f"The characters following '{compoundfile2}' we found: '{filenumbers_compound_2}'")
        else:
            print(f"The word '{result}' was not found in the text.")

        search_file = "_split_" + filenumbers_compound_2
        for filepath in reaction_files:
            filename = filepath.split("/")[-1]
            if search_file in filename:
                reaction_file_to_rename = filepath
                break
        
        print(f'our matches: {reaction_file_to_rename}')

        # rename the second reactionfiles accordingly to the compoundfile
        reaction_file_df = lazyread_mines_parquet(reaction_file_to_rename)

        # # reaction_file_df['ID equation'] = reaction_file_df['ID equation'].replace(compounds_dict_to_rename)
        # reaction_file_df = reaction_file_df.with_columns(pl.col(['ID equation']).replace(compounds_dict_to_rename))
        # print(reaction_file_df)
        # reaction_file_df1 = reaction_file_df.filter(pl.col(["ID equation"]).str.contains("_00]", literal=True))
        # print(reaction_file_df1)

        # values_to_find = list(compounds_dict_to_rename.values())
        # reaction_file_df1 = reaction_file_df.filter(pl.col(["ID equation"]).str.contains_any(values_to_find))
        # print(reaction_file_df1)

        # Construct the replacement expression for each column
        reaction_file_df1 = reaction_file_df.with_columns(
        pl.col(["ID equation"]).str.replace_many(
            second_compound_duplicates, first_compound_duplicates
        )
        .alias("ID equation")
        )

        reaction_file_df2 = reaction_file_df1.filter(pl.col(["ID equation"]).str.contains_any(first_compound_duplicates)).collect()
        print("renamed reaction file:", reaction_file_df2)

        # if duplicates in the reactions rules are find, drop them


        reaction_file_df1.sink_parquet(reaction_file_to_rename)
        print(f"reaction saved: {reaction_file_to_rename}")

        # Qualitycheck, to see how many are changed and all the removed duplicates are removed.
        removed_compounds.append(second_compound_duplicates)
        renamed_compounds.append(first_compound_duplicates)

        # reaction_file = reaction_files[]
        # df = lazyread_mines_parquet(reaction_file)
        # print(df)
        # df = df.with_column(pl.col("ID equation").apply(lambda x: replace_chars(x, compounds_dict_to_rename)).alias("text"))
        # print(df)

        
