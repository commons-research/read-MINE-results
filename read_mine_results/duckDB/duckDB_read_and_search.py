import polars as pl
import duckdb
import ray
import glob, os, time

folder_path = "../MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/results/"
parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))

# ray init : Specify the number of workers and the amount of memory (500 Gb) each can use
ray.init(num_cpus=5, object_store_memory=500e9)

# Define a function to process a chunk of data
@ray.remote
def process_chunk(chunk_df):
    # Create an in-memory DuckDB connection
    con = duckdb.connect(database=':memory:')
    
    # Register the Polars DataFrame as a DuckDB relation
    con.register("chunk_df", chunk_df)
    
    # Create a table from the chunk and perform a query
    con.execute('CREATE TABLE data AS SELECT * FROM chunk_df')
    result = con.execute("SELECT COUNT(*) FROM data WHERE ID = 'http://www.wikidata.org/entity/Q66311060'").fetchone()
    
    # give back the possible columns to be searched
    # Query to get table information
    table_info = con.execute('PRAGMA table_info(data)').fetchall()

    # Print column names and their data types
    print("Columns in the 'data' table:")
    for col in table_info:
        print(f"Column: {col[1]}, Data Type: {col[2]}")

    # Close the connection
    con.close()
    
    return result[0]

start_time = time.time()

# Load the large dataset with Polars
df = pl.read_parquet(parquet_files)  

read_in_time = time.time() - start_time

# Split the DataFrame into chunks
num_chunks = 4  # Number of chunks (can be adjusted based on your needs)
chunks = df.chunked(num_chunks)

# Distribute the chunks to Ray workers
futures = [process_chunk.remote(chunk) for chunk in chunks]

# Collect the results from all workers
results = ray.get(futures)

# Sum the results from all chunks
total_count = sum(results)

print(f'Total count of users older than 25: {total_count}')

# Shutdown Ray
ray.shutdown()


print_time = time.time() - read_in_time - start_time

print(f'time used: \nread_in_and_processed_time: {read_in_time:.2f}sec \tshow_time: {print_time:.2f}sec')
