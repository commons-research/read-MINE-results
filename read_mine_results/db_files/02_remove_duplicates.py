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

pl.Config(fmt_str_lengths=550)



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

# find the pairs and put them togheter
for compoundfile1 in compound_files:

    for compoundfile2 in compound_files:

        # if the same file, go to the next file
        if compoundfile1 == compoundfile2:
            continue

        # read both files at once
        lf_compound = lazyread_mines_parquet([compoundfile1, compoundfile2])

        # duplicate is not in lazyframe (collecting step should be in the end if possible)
        df = lf_compound.select(['ID', 'Formula', 'InChIKey', 'SMILES']).collect(streaming = True)
        
        # columns for compounds: ['ID', 'Type', 'Generation', 'Formula', 'InChIKey', 'SMILES']
        cp_columns_equal = ['Formula', 'InChIKey', 'SMILES']

        # find duplicates in compounds
        duplicated_compounds = df.filter(pl.lit(df.select(cp_columns_equal).is_duplicated())).sort(cp_columns_equal)

        # get the unique compounds
        compounds_unique = lf_compound.select(['ID', 'Formula', 'InChIKey', 'SMILES']).unique(subset=cp_columns_equal).sort(cp_columns_equal).collect(streaming = True)


        # df_duplicated_compounds = lf_compound.filter(pl.all_horizontal(pl.col(cp_columns_equal).is_duplicated())).collect(streaming = True)
        # df_unique_compounds = lf_compound.unique(subset=cp_columns_equal).collect(streaming = True)
 
        

        # df_compound_duplicate = df_compound.filter(df_compound.is_duplicated(cp_columns_equal))

        
        # duplicates_in_unique = duplicated_compounds.join(compounds_unique, on=cp_columns_equal, how="inner").sort(cp_columns_equal)

        # print("Duplicates:", duplicates_in_unique)

        print(df.shape, duplicated_compounds)
        print(compounds_unique)

        # 'pkc19930364_00': 'pkc19395400_01', 'pkc3595900_01': 'pkc3693228_00', 'pkc18101948_01': 'pkc18601703_00'

        # duplicates_df = lf_compound.group_by(cp_columns_equal).agg(pl.count().alias("count")).filter(pl.col("count") > 1).select(cp_columns_equal).collect()
        duplicates_df = lf_compound.group_by(cp_columns_equal).agg(pl.len().alias("count")).filter(pl.col("count") > 1).select("count").unique().collect()
        
        if (duplicates_df.item() == 2):

            # Extract the first and the second values from duplicates
            # Get odd rows
            first_values = duplicated_compounds.with_row_index().filter(pl.col("index") % 2 == 1)["ID"].to_list()

            # Get even rows
            second_values = duplicated_compounds.with_row_index().filter(pl.col("index") % 2 == 0)["ID"].to_list()

            # Create a dictionary from the two lists
            duplicates_dict = dict(zip(first_values, second_values))


            print(duplicates_dict)

            # rename the reactions
            # load reactions
            identifier_compound = find_two_chars_after_word(compoundfile1, "split_")

            for reactionfile in reaction_files:

                identifier_reaction = find_two_chars_after_word(reactionfile, "split_")

                if identifier_reaction == identifier_compound:
                    
                    lf_reaction = lazyread_mines_parquet([compoundfile1, compoundfile2])



                    # drop the value out of the list, if the pair was found
                    reaction_files.remove(reactionfile)


            lf_reaction = lazyread_mines_parquet([compoundfile1, compoundfile2])

            df 

        else:
            sys.exit("More duplicates then expected:", duplicates_df.item() == 2)


        # if (duplicates_df.item() == 2) and (duplicates_df.shape == (1, 1)):  
        # 'pkc20932279_00': 'pkc20368597_01', 'pkc19930364_00': 'pkc19395400_01', 'pkc3595900_01': 'pkc3693228_00', 'pkc18101948_01': 'pkc18601703_00'
        #     entries = duplicated_compounds.select(pl.col("ID")).item()
        #     print("Yes, you are right!!!", entries)

        #     correspondence_dict = {entries[i]: entries[i + 1] for i in range(0, len(entries), 2)}

        # Drop duplicates from the original LazyFrame
        # unique_df = lf_compound.unique()

        # unique_df = unique_df.collect()

        # cp_dp_am = cp_dp_am.shape[0]
        # cp_un_am = df_unique_compounds.shape[0]

        # print(f"duplicated compounds: {cp_dp_am}")
        # print(f"unique compounds: {cp_un_am}")

        # df_compound = lf_compound.collect(streaming = True)
        # print(f'total: {df_compound.shape} - [ {cp_dp_am + cp_un_am} ]')






# for reactionfile in reaction_files:
#     # read file
#     lf_reaction = lazyread_mines_parquet(reactionfile)

#     # ['ID', 'Type', 'Generation', 'Formula', 'InChIKey', 'SMILES']
#     re_columns_equal = ['Formula', 'InChIKey', 'SMILES']
    
# ['ID', 'Name', 'ID equation', 'SMILES equation', 'Rxn hash', 'Reaction rules']
# cp_columns_equal = ['ID equation', 'SMILES equation', 'Rxn hash', 'Reaction rules']

# #     # find duplicates in reactions 

    
    
    
    
#     if identifier_compound == identifier_reaction:
            
            
#             rename_compounds(path_to_files + compoundfile, suffix = "_" + identifier_compound, output_file = path_to_save + compoundfile)
#             rename_reactions(path_to_files + reactionfile, suffix = "_" + identifier_reaction, output_file = path_to_save + reactionfile)
        
#             # drop the value out of the list, if the pair was found
#             reaction_files.remove(reactionfile)
