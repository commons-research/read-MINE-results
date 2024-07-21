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
import os, sys

# module_path = '/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results' 
# module_path = '/home/popeye/2024_GitHub_Master_Bioinformatics/read-MINE-results/read_mine_results' 
module_path = '/home/admin/2024_master/read-MINE-results/read_mine_results' 

module_path = os.path.abspath(module_path)
if module_path not in sys.path:
    sys.path.append(module_path)
import read_mine as rm


path_to_files = sys.argv[1]
path_to_save = sys.argv[2]


pl.Config(fmt_str_lengths=550)



# get all filenames with path
filenames = rm.get_files_in_folder(path_to_files)

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

    identifier_compound = rm.find_two_chars_after_word(compoundfile, "split_")

    for reactionfile in reaction_files:
        identifier_reaction = rm.find_two_chars_after_word(reactionfile, "split_")

        # if the pair is found
        if identifier_compound == identifier_reaction:
            # rename the files
            rm.rename_compounds(path_to_files + compoundfile, suffix = "_" + identifier_compound, output_file = path_to_save + compoundfile)
            rm.rename_reactions(path_to_files + reactionfile, suffix = "_" + identifier_reaction, output_file = path_to_save + reactionfile)
          
            # drop the value out of the list, if the pair was found
            reaction_files.remove(reactionfile)

            print(f'renamed files: {compoundfile}\t\t{reactionfile}')