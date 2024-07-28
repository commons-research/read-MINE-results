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
import os, sys, math

module_path = '/home/pamrein/2024_masterthesis/read-MINE-results/read_mine_results' 
# module_path = '/home/popeye/2024_GitHub_Master_Bioinformatics/read-MINE-results/read_mine_results' 
# module_path = '/home/admin/2024_master/read-MINE-results/read_mine_results' 

module_path = os.path.abspath(module_path)
if module_path not in sys.path:
    sys.path.append(module_path)
import read_mine as rm

# get the folder via terminal
path_to_files = sys.argv[1]
path_to_save = sys.argv[2]

pl.Config(fmt_str_lengths=550)
# pl.Config.set_tbl_rows(100)

# get all filenames with path
filenames = rm.get_files_in_folder(path_to_files)
filename_in_output_path = rm.existing_files(path_to_save)

for file in filenames:
    # get the filename (last element)
    filename = file.split("/")[-1]

    if filename in filename_in_output_path:
        continue
    
    # get all the compounds
    elif "reactions" in filename:
        rm.drop_and_save(file, columns_to_drop = ["Name"], save_in_folder = path_to_save)

    # get all the reactions
    elif "compounds" in filename:
        rm.drop_and_save(file, columns_to_drop = ["Generation"], save_in_folder = path_to_save)

print(f"files saved in: {path_to_save}")

filenames_new = rm.get_files_in_folder(path_to_files)

# filter out the compound and reaction files
reaction_files = []
compound_files = []

for file in filenames_new:
    # get the filename (last element)
    filename = file.split("/")[-1]
    
    # get all the compounds
    if "reactions" in filename:
        reaction_files.append(file)

    # get all the reactions
    if "compounds" in filename:
        compound_files.append(file)

print(f"""files to remove duplicates: {len(reaction_files)} reaction files and {len(compound_files)} compound files.
total iterations: {math.comb(len(compound_files), 2)}""")


# sort the lists
reaction_files.sort()
compound_files.sort()

compound_files2 = compound_files.copy()

# for the statistics in the end
removed_compounds = list()
renamed_compounds = list()
removed_reactions = list()

# go through all the files
for compoundfile1 in compound_files:

    # remove the compound from the list for better performance (don't check yourself)
    compound_files2.remove(compoundfile1)

    for compoundfile2 in compound_files2:
        
        # get the filenumbers of this files
        filenumber_compound_1 = rm.find_two_chars_after_word(compoundfile1, word = "_split_")
        filenumber_compound_2 = rm.find_two_chars_after_word(compoundfile2, word = "_split_")

        # compounds list which have to be renamed
        first_compound_duplicates, second_compound_duplicates = rm.get_merged_file(compoundfile1, compoundfile2, info = True, equal_columns = ["Formula", "InChIKey", "SMILES"])

        # If no compounds have to be renamed, also the reactionfiles doesn't have to be renamed
        if not first_compound_duplicates: #not second_compound_duplicates and
            print("no compounds found to rename --> reactions and compounds would not be changed.")

        else:
            # drop all duplicates from the second file (compoundfile2)
            lf_compoundfile2 = rm.lazyread_mines_parquet(compoundfile2)
            lf_compoundfile2 = lf_compoundfile2.filter(~pl.col("ID").is_in(second_compound_duplicates))

            # get the filename (last element)
            filename_compound = compoundfile2.split("/")[-1]

            # overwrite the existing file
            lf_compoundfile2.sink_parquet(path_to_save + filename_compound + "_removed_duplicates.parquet")
            os.rename(path_to_save + filename_compound + "_removed_duplicates.parquet", path_to_save + filename_compound)

            # find the reaction file to rename it
            reaction_file_to_rename = rm.find_file_in_list(files = reaction_files, number = filenumber_compound_2)

            # rename the second reactionfiles accordingly to the compoundfile
            reaction_file_lf = rm.lazyread_mines_parquet(reaction_file_to_rename)

            # rename the column "ID equation" with the *updated* compounds
            reaction_file_lf_renamed = reaction_file_lf.with_columns(
                pl.col(["ID equation"])
                .str.replace_many(
                    second_compound_duplicates, first_compound_duplicates
                )
                .alias("ID equation")
            )

            # find the other reaction file
            reaction_file = rm.find_file_in_list(files = reaction_files, number = filenumber_compound_1)

            # find duplicates in the reactions
            first_reactions_duplicates, second_reactions_duplicates = rm.get_merged_file(reaction_file, reaction_file_to_rename, 
                equal_columns = ['SMILES equation', 'Rxn hash', 'Reaction rules'], 
                info = True
                )

            #drop the duplicates from the renamed reaction file
            reactions_cleaned = reaction_file_lf_renamed.filter(~pl.col("ID").is_in(second_reactions_duplicates))

            # get the filename (last element)
            filename_reaction = reaction_file_to_rename.split("/")[-1]

            reactions_cleaned.sink_parquet(path_to_save + filename_reaction + "_removed_duplicates.parquet")
            os.rename(path_to_save + filename_reaction + "_removed_duplicates.parquet", path_to_save + filename_reaction)

            # Qualitycheck, to see how many are changed and all the removed duplicates are removed.
            removed_reactions.extend(second_reactions_duplicates)
            removed_compounds.extend(second_compound_duplicates)
            renamed_compounds.extend(first_compound_duplicates)

print(f'---[ final statistics ]---')
print(f'removed reactions: {len(removed_reactions)}')
print(f'removed compounds: {len(removed_compounds)}')
print(f'renamed compounds: {len(renamed_compounds)}')
