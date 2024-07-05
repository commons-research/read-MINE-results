import dask.dataframe as dd
import polars as pl
import time

# start with:
# srun --partition=pibu_el8 --cpus-per-task=10 --mem=900G --time=10:00:00 --pty /bin/bash"
# micromamba run -n read-MINEs python read_mine_results/dask/main_dask.py > data/ray/output_ray.log


# change to specific preferences
search_structure = 'http://www.wikidata.org/entity/Q66311060'
write_to_parquet = True
output_dir = "/home/pamrein/2024_masterthesis/read-MINE-results/data/dask/" 
print(f"Search for: {search_structure}\nwith dask")

# Path to the directory containing Parquet files (be careful, the path should be from the place, where the script will be started)
parquet_directory = "../MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/results/"

start_time = time.time()

# Read multiple Parquet files in parallel
df = dd.read_parquet(parquet_directory)


read_in_time = time.time() - start_time

# Perform some operations, e.g., filter rows where 'column_name' > some_value
# >>> df.columns
# ['ID', 'predicted_reaction_ID equation', 'predicted_reaction_SMILES equation', 'predicted_reaction_Rxn hash', 'predicted_reaction_Reaction rules', 
# 'predicted_compounds_Formula', 'predicted_compounds_InChIKey', 'predicted_compounds_SMILES']
df_filtered = df[df['ID'] == search_structure]

filtered_time = time.time() - read_in_time - start_time


# Compute the result and convert to a pandas DataFrame
df_result = df_filtered.compute()

# Show results                                                                                         │
print("pandas:", df_result)  


# Save if we want
if write_to_parquet:
    # Get the current time
    current_time = time.localtime()

    # Format the time as a string
    time_str = time.strftime("%Y%m%d_%H%M%S", current_time)

    # write to file
    df_result.to_parquet(output_dir, name_function=lambda i: f"data_dask-{i}.parquet", write_metadata_file=True)

show_time = time.time() - filtered_time - read_in_time - start_time

print(f'time used: \nread_in_time: {read_in_time:.2f}sec \tfiltered_time: {filtered_time:.2f}sec \tshow_time: {show_time:.2f}sec')

