import polars as pl
import duckdb
import numpy
import ray
import os, glob, time


# Get a list of all Parquet files in the directory
folder_path = "../MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/results/"
parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))

# Initialize Ray : Specify the number of workers and the amount of memory (500 Gb) each can use
ray.init(num_cpus=5, object_store_memory=500e9)

# Define a function to process a chunk of data
@ray.remote
def process_parquet_file(file_path):
    # Read the Parquet file with Polars
    chunk_df = pl.read_parquet(file_path)
    
    # Create an in-memory DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Register the Polars DataFrame as a DuckDB relation
    con.register("chunk_df", chunk_df)
    
    # Create a table from the chunk and insert data
    con.execute('CREATE TABLE IF NOT EXISTS data AS SELECT * FROM chunk_df')
    con.execute('INSERT INTO data SELECT * FROM chunk_df')
    
    # Export the table back to Polars DataFrame
    result_df = con.execute('SELECT * FROM data').fetchdf()
    
    # Close the connection
    con.close()
    
    return result_df


# Process the Parquet files in parallel using Ray
futures = [process_parquet_file.remote(file) for file in parquet_files]
results = ray.get(futures)

print("all files loaded")

# Combine all results into a single Polars DataFrame
combined_df = pl.concat(results)

print("all files concatenated")

# Save the combined DataFrame to a DuckDB database
db_name = 'db_lotus_expanded.db'
con = duckdb.connect(database=db_name)
con.register("combined_df", combined_df)
con.execute('CREATE TABLE data AS SELECT * FROM combined_df')

print("DB created")

# Create an index on the ID column
con.execute('CREATE INDEX id_index ON data (ID)')

print("Index created")

# Close the connection
con.close()

# Shutdown Ray
ray.shutdown()