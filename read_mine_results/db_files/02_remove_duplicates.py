# Description:
# Rename the names of the compounds and reactions, so they are unique and differantiable to the other tables.
# The main reason is, because pickaxe will name the pkc and pkr just from 1 to ...


# time to run for 3 files:
# real    6m6.483s
# user    9m32.428s
# sys     1m7.996s

# files for renaming should be in one folder (renamed files will be recognized)
# path_to_files = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/removed_duplicates/" #"/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/renamed_files/"
# path_to_save = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/removed_duplicates/"



import polars as pl
import os
import re
import sys
import re

# get the folder via terminal
path_to_files = sys.argv[1]
path_to_save = sys.argv[2]

pl.Config(fmt_str_lengths=550)
# pl.Config.set_tbl_rows(100)


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


def find_two_chars_after_word(text, word = "_split_"):
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


def find_file_in_list(files, number, prefix = "_split_"):
    # not implemented: how handle a case with multiple "search_pattern"
    # search pattern
    search_pattern = prefix + number

    # go through all the files
    for filepath in files:
        filename = filepath.split("/")[-1]
        if search_pattern in filename:
            file = filepath
            return file
    return None


# def reorder(row):
#     col1_value, col2_value = row[0], row[1]
#     col1_suffix = int(col1_value.split('_')[1])
    
#     if col1_suffix == "02":
#         return col1_value
#     else:
#         return col2_value
        

def find_duplicates_df(files = list(), polar_lf = None, equal_columns = "", info = False):
    if files:
        df = lazyread_mines_parquet(files).collect(streaming = True)
    else:
        df = polar_lf.collect(streaming = True)

    # if no columns provided, take all of them
    if not equal_columns:
        equal_columns = df.columns
        print(f"All columns taken: {equal_columns: _d}")

    df_duplicates = df.filter(pl.lit(df.select(equal_columns).is_duplicated())).sort(equal_columns)

    if info:
        print(f'loaded over all files: {df.shape[0] : _d} \nduplicates founded: {df_duplicates.shape[0] : _d}')

    return df_duplicates


def get_merged_compoundfile(compoundfile, merged_in_compoundfile, equal_columns = ['Formula', 'InChIKey', 'SMILES'], info = False):
    # load the files in lazyframe
    lf_compound = lazyread_mines_parquet([compoundfile, merged_in_compoundfile])

    # find duplicates in compounds
    duplicated_compounds = find_duplicates_df(polar_lf = lf_compound, equal_columns = equal_columns)

    check_if_only_duplicates = lf_compound.group_by(equal_columns).agg(pl.len().alias("count")).select("count").filter(pl.col("count") > 1).unique().collect(streaming = True)

    print("check_if_only_duplicates", check_if_only_duplicates, type(check_if_only_duplicates))

    if check_if_only_duplicates.shape != (1, 1):
        print(f"shape doesn't match: {check_if_only_duplicates.shape}")
        first_compound_duplicates = list()
        second_compound_duplicates = list()
    
    elif (int(check_if_only_duplicates.item()) == 2):
        # Because the find_duplicate function searches sorts the compounds depending on the column and we are sure, that only duplicates are found.
        # This is why we here look for the odd and even numbers and sort them depending on that.

        # Add an index column
        duplicated_compounds = duplicated_compounds.with_row_index("index")

        # Filter for odd indices
        odd_indices_df = duplicated_compounds.filter(pl.col("index") % 2 != 0).select(pl.col("ID").alias("compound_1"))

        # Filter for even indices
        even_indices_df = duplicated_compounds.filter(pl.col("index") % 2 == 0).select(pl.col("ID").alias("compound_2"))

        # Combine the two DataFrames
        combined_df = pl.concat([odd_indices_df, even_indices_df], how="horizontal")

        # reorder the files        
        filenumber_compound_1 = find_two_chars_after_word(compoundfile)

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

        # Get odd rows
        first_compound_duplicates = duplicated_compounds_df["compound_1"].to_list() 

        # Get even rows
        second_compound_duplicates = duplicated_compounds_df["compound_2"].to_list() 

    elif (int(check_if_only_duplicates.item()) == 1):
        first_compound_duplicates = list()
        second_compound_duplicates = list()

    else:
        sys.exit("More duplicates then expected:", check_if_only_duplicates)


    if info:
        all_compounds = lf_compound.collect(streaming = True).shape[0]
        duplicated_compounds = int(duplicated_compounds.shape[0] / 2)
        unique_compounds = lf_compound.select(equal_columns).unique().collect(streaming = True).shape[0]

        filename_1 = compoundfile.split("/")[-1]
        filename_2 = merged_in_compoundfile.split("/")[-1]

        print(f'File 1: {filename_1},\nFile 2: {filename_2}\nTotal: {all_compounds:_d}\nunique compounds: {unique_compounds:_d} ({duplicated_compounds/all_compounds*100:.2f} % - {duplicated_compounds:_d} duplicates)\n-----')

    return first_compound_duplicates, second_compound_duplicates


def drop_and_save(file, columns_to_drop=list()):
    try:
        # Scan the Parquet file into a LazyFrame
        lf = pl.scan_parquet(file)
        existing_columns = lf.columns
        print(f"\nExisting columns: {existing_columns} | File: {file}")
        
        if columns_to_drop:
            # Check if any columns to drop exist in the LazyFrame
            columns_to_drop = [col for col in columns_to_drop if col in existing_columns]
            
            if columns_to_drop:
                # Drop the specified columns using LazyFrame API
                lf = lf.drop(columns_to_drop)

                tmp = lf.collect()

                tmp.write_parquet(file)

                print("Updated columns:", tmp.columns)
                return tmp
            else:
                print("No columns to drop found in the LazyFrame.")
                # Collect the original LazyFrame and return it
                return None
        else:
            print("No columns specified to drop.")
            # Collect the original LazyFrame and return it
            return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


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
        drop_and_save(file, columns_to_drop = ["Name"])

    # get all the reactions
    if "compounds" in filename:
        compound_files.append(file)
        drop_and_save(file, columns_to_drop = ["Generation"])

# sort the lists
reaction_files.sort()
compound_files.sort()

compound_files2 = compound_files.copy()

removed_compounds = list()
renamed_compounds = list()
removed_reactions = list()

# go through all the files
for compoundfile1 in compound_files:

    # remove the compound from the list for better performance (don't check yourself)
    compound_files2.remove(compoundfile1)

    for compoundfile2 in compound_files2:
        
        # get the filenumbers of this files
        filenumber_compound_1 = find_two_chars_after_word(compoundfile1, word = "_split_")
        filenumber_compound_2 = find_two_chars_after_word(compoundfile2, word = "_split_")

        # compounds list which have to be renamed
        first_compound_duplicates, second_compound_duplicates = get_merged_compoundfile(compoundfile1, compoundfile2, info = True)

        # If no compounds have to be renamed, also the reactionfiles doesn't have to be renamed
        if not first_compound_duplicates: #not second_compound_duplicates and
            print("no compounds found to rename --> reactions and compounds would not be changed.")

        else:
            # drop all duplicates from the second file (compoundfile2)
            df_compoundfile2 = lazyread_mines_parquet(compoundfile2)
            df_compoundfile2 = df_compoundfile2.filter(~pl.col("ID").is_in(second_compound_duplicates))

            # overwrite the existing file
            df_compoundfile2.sink_parquet(compoundfile2 + "_removed_duplicates.parquet")
            os.rename(compoundfile2 + "_removed_duplicates.parquet", compoundfile2)

            # find the reaction file to rename it
            reaction_file_to_rename = find_file_in_list(files = reaction_files, number = filenumber_compound_2)

            # rename the second reactionfiles accordingly to the compoundfile
            reaction_file_lf = lazyread_mines_parquet(reaction_file_to_rename)

            # rename the column "ID equation" with the *updated* compounds
            reaction_file_lf_renamed = reaction_file_lf.with_columns(
                pl.col(["ID equation"])
                .str.replace_many(
                    second_compound_duplicates, first_compound_duplicates
                )
                .alias("ID equation")
            )

            # find the other reaction file
            reaction_file = find_file_in_list(files = reaction_files, number = filenumber_compound_1)

            # find duplicates in the reactions
            duplicated_reactions = find_duplicates_df(files = [reaction_file, reaction_file_to_rename], 
                equal_columns = ['ID equation', 'SMILES equation', 'Rxn hash', 'Reaction rules'], 
                info = True
                )

            # get a list of duplicates from the file which is renamed
            duplicated_reactions = duplicated_reactions["ID"].str.ends_with("_" + filenumber_compound_2).to_list()

            #drop the duplicates from the renamed reaction file
            reactions_cleaned = reaction_file_lf_renamed.filter(~pl.col("ID").is_in(duplicated_reactions))

            reactions_cleaned.sink_parquet(reaction_file_to_rename + "_removed_duplicates.parquet")
            os.rename(reaction_file_to_rename + "_removed_duplicates.parquet", reaction_file_to_rename)

            # Qualitycheck, to see how many are changed and all the removed duplicates are removed.
            removed_reactions.extend(duplicated_reactions)
            removed_compounds.extend(second_compound_duplicates)
            renamed_compounds.extend(first_compound_duplicates)

print(f'removed reactions: {len(removed_reactions)}')
print(f'removed compounds: {len(removed_compounds)}')
print(f'renamed compounds: {len(renamed_compounds)}')
