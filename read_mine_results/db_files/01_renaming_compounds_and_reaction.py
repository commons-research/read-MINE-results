# Description:
# Rename the names of the compounds and reactions, so they are unique and differantiable to the other tables.
# The main reason is, because pickaxe will name the pkc and pkr just from 1 to ...


# time to run for 3 files:
# real    6m6.483s
# user    9m32.428s
# sys     1m7.996s

# files for renaming should be in one folder (renamed files will be recognized)
# path_to_files = "/home/pamrein/2024_masterthesis/MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/"
# path_to_save = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/"

# path_to_files = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/all_original_files/"
# path_to_save = "/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results/db_files/all_original_files/"


import polars as pl
import os
import re
import sys

path_to_files = sys.argv[1]
path_to_save = sys.argv[2]


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
        print(f'other file')
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


def rename_compounds(compoundfile, suffix, output_file = ""):
    lf_compounds = lazyread_mines_parquet(compoundfile)

    # Append the suffix to all values in the specified column
    lf_compounds = lf_compounds.with_columns(
        (pl.col("ID") + suffix).alias("ID")
    )

    # Collect the lazy frame to execute the operations
    df_compounds = lf_compounds.collect()

    if output_file:
        df_compounds.write_parquet(output_file)
    
    return 0


def rename_reactions(reactionfile, suffix, output_file = ""):

    # read in reactionfile
    lf_reactions = lazyread_mines_parquet(reactionfile)

    # Append the suffix to all values in the column "ID"
    lf_reactions = lf_reactions.with_columns(
        (pl.col("ID") + suffix).alias("ID")
    )

    # Regex pattern to find 'pkc' followed by digits and '[c0]'
    pattern = r"(pkc\d+)(\[c)" 

    # Define the replacement expressions for the column "ID equation"
    replacement_expr_pkc = pl.col("ID equation").str.replace_all(pattern, r"${1}_XXX-pkc-XXX_${2}") 
 
    replacement_expr_suffix = pl.col("ID equation").str.replace_all(r"_XXX-pkc-XXX_", suffix) 

    # Apply the replacement expression to the specified column
    lf_reactions = lf_reactions.with_columns(replacement_expr_pkc.alias("ID equation"))
    lf_reactions = lf_reactions.with_columns(replacement_expr_suffix.alias("ID equation"))
    
    df_reactions = lf_reactions.collect()

    if output_file:
        df_reactions.write_parquet(output_file)

    return df_reactions


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
        reaction_files.append(filename)

    # get all the reactions
    if "compounds" in filename:
        compound_files.append(filename)


# find the pairs and put them togheter
for compoundfile in compound_files:

    identifier_compound = find_two_chars_after_word(compoundfile, "split_")

    for reactionfile in reaction_files:
        identifier_reaction = find_two_chars_after_word(reactionfile, "split_")

        if identifier_compound == identifier_reaction:
            # rename the files
            rename_compounds(path_to_files + compoundfile, suffix = "_" + identifier_compound, output_file = path_to_save + compoundfile)
            rename_reactions(path_to_files + reactionfile, suffix = "_" + identifier_reaction, output_file = path_to_save + reactionfile)
          
            # drop the value out of the list, if the pair was found
            reaction_files.remove(reactionfile)