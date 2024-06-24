import ray
import polars as pl
import time
import glob, os

# Specify the number of workers and the amount of memory (500 Gb) each can use
ray.init(num_cpus=5, object_store_memory=500e9)

# Define a function that uses Polars for data processing
def process_data(file_path):
    # Read data with Polars
    df = pl.read_parquet(file_path)
    
    # Perform some data transformations with Polars
    transformed_df = df.filter(pl.col('ID') == 'http://www.wikidata.org/entity/Q66311060')
    
    # Return the result as a Polars DataFrame
    return transformed_df

# List of Parquet files
folder_path = "../MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/results/"
parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))

# Distribute tasks across Ray cluster
@ray.remote
def process_file_remote(file_path):
    return process_data(file_path)

start_time = time.time()

# Execute tasks asynchronously
results = ray.get([process_file_remote.remote(file) for file in parquet_files])
print(results)

read_in_time = time.time() - start_time

# Concatenate results if needed
final_result = pl.concat(results)

# Shutdown Ray
ray.shutdown()

# Output or further process final_result
print(final_result)

print_time = time.time() - read_in_time - start_time

print(f'time used: \nread_in_and_processed_time: {read_in_time:.2f}sec \tshow_time: {print_time:.2f}sec')

