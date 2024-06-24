import dask.dataframe as dd
import polars as pl
import time


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
df_filtered = df[df['ID'] == 'http://www.wikidata.org/entity/Q66311060']

filtered_time = time.time() - read_in_time - start_time


# Compute the result and convert to a pandas DataFrame
result = df_filtered.compute()

# Show results                                                                                         │
print("pandas:", result)  


show_time = time.time() - filtered_time - read_in_time - start_time


# Convert Pandas DataFrame to Polars DataFrame
polars_df = pl.from_pandas(result) 

# Show results
print("polars:", polars_df)


print(f'time used: \nread_in_time: {read_in_time:.2f}sec \tfiltered_time: {filtered_time:.2f}sec \tshow_time: {show_time:.2f}sec')

