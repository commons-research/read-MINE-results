import dask.dataframe as dd
import polars as pl
import duckdb


# Define the path to your Parquet files directory
parquet_dir = 'path_to_parquet_files'  # Replace with your directory path

# Load Parquet files into a Dask DataFrame
ddf = dd.read_parquet(parquet_dir)

# Define the path to your DuckDB database file
db_name = 'db_lotus_expanded.db'

# Function to store a Dask DataFrame chunk into DuckDB
def store_chunk(chunk, db_name):
    con = duckdb.connect(database=db_name)
    con.execute('CREATE TABLE IF NOT EXISTS data AS SELECT * FROM chunk LIMIT 0')
    con.execute('INSERT INTO data SELECT * FROM chunk')
    con.close()

# Convert Dask DataFrame to Pandas DataFrame chunks and store in DuckDB
for i, chunk in enumerate(ddf.to_delayed()):
    print(f"Processing chunk {i+1}")
    chunk_df = chunk.compute()
    store_chunk(chunk_df, db_name)

# Connect to DuckDB to create an index after all data is inserted
con = duckdb.connect(database=db_name)
con.execute('CREATE INDEX IF NOT EXISTS id_index ON data (ID)')
con.close()
