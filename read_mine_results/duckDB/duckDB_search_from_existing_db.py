# Example: read in db and search for...
import duckdb

# path to database
path_database = '/read_mine_results/duckDB/db_lotus_expanded.db'

# Connect to the existing DuckDB database
con = duckdb.connect(database=path_database)

print(f'Database is readed in: {path_database}')

# Query the data for the specific ID
query = "SELECT * FROM data WHERE ID = 'http://www.wikidata.org/entity/Q66311060'"
result = con.execute(query).fetchall()

# Print the result
for row in result:
    print(row)

# Close the connection
con.close()
