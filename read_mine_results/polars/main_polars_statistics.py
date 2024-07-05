import polars as pl
import sys, time

# to modify
path = "/home/pamrein/2024_masterthesis/MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/compounds_1_generalized_230106_frozen_metadata_for_MINES_split_00.csv_77226.91_.parquet"
path = "/home/pamrein/2024_masterthesis/MINE-Database/data/output/metacyc_generalized/20240601_lotus_generalized_n45/compounds_1_generalized_230106_frozen_metadata_for_MINES_split_*1.csv*.parquet"

column_name = "ID"


def check_element_in_list(element, lst, verbose=False):
    """
    check, if a given column name exist in a list.
    """
    try:
        if element not in lst:
            if verbose:
                print(f"Element {element} not found in the list. \nPlease choose from the following columns:\n{lst}")
            sys.exit(1)  # Exit the script with a non-zero status
        else:
            if verbose:
                print(f"Element {element} is in the list.")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


def duplicates_amount(lazy_df, column_to_search=""):
    """
    """
    all_columns = df_predicted.columns

    # Specify the column you want to check for duplicates
    if not column_to_search:
        column_to_search = all_columns
    else:
        check_element_in_list(column_to_search, all_columns)

    # Find duplicates and collect them (for lazy dataframes)
    df_duplicates = df_predicted.group_by(column_to_search).agg(pl.len()).filter(pl.col("len") > 1)
    df_duplicates_collected = df_duplicates.collect()

    return df_duplicates_collected

def check_for_blanks(lazy_df):
    """
    check for Nulls / None / empty entries. Just provide an lazy dataframe
    """

    # Define a filter condition for the LazyFrame
    filter_condition = (
        lazy_df
        .for_each(lambda df: df.filter(
            pl.col("*").is_not_null() &  # Exclude rows with any null values
            ~pl.col("*").cast(str).str.strip().is_empty()  # Exclude rows with any empty strings
        ))
    )

    # Collect the filtered LazyFrame into a concrete DataFrame
    filtered_df = filter_condition.collect()

    return filtered_df


# Start the timer
start_time = time.time()

print(f'following files will be searched: \n{path}')


# load in with polars and filter out only the predicted -> Lazydataframe
df_predicted = (
    pl.scan_parquet(path).filter(pl.col("Type") == "Predicted")
    )

for col_df in df_predicted.columns:

    # search for duplicates (gives back a dataframe - collected)
    df_duplicates_collected = duplicates_amount(df_predicted, column_to_search = col_df)

    # Print the duplicates
    print(f'duplicates in {col_df}\n\n{df_duplicates_collected}\n---\n')


df_blanks = check_for_blanks(df_predicted)
print(f'Entries with blanks: {df_blanks}')

# End the timer
end_time = time.time()

# Calculate the elapsed time
elapsed_time = end_time - start_time

print(f"Elapsed time: {elapsed_time: .2f} seconds")