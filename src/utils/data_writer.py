import os
import pyarrow as pa
import pyarrow.parquet as pq

def save_data_to_parquet(df, output_path, year, month, day):
    """
    Save the DataFrame to a Parquet file in the specified output directory.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_path (str): Directory where the Parquet file will be saved.
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
    """
    print(f"Saving data for {year}-{month:02d}-{day:02d} to Parquet")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    output_filename = f"{output_path}/{year}-{month:02d}-{day:02d}_precipitation_data.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filename)

def save_query_results_to_parquet(df, query_name, output_dir):
    """
    Save filtered query results to a Parquet file in the specified output directory.

    Args:
        df (pd.DataFrame): Filtered query results to save.
        query_name (str): Identifier for the query (used in file naming).
        output_dir (str): Directory to save the query results.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Define output file name based on query name
    output_filename = f"{output_dir}/{query_name}_filtered_data.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_filename)
    print(f"Saved query result to {output_filename}")