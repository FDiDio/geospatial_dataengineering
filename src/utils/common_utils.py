import os
import pandas as pd

def save_query_results_to_parquet(df, query_name, output_dir):
    """
    Save the filtered DataFrame to a Parquet file.

    :param df: DataFrame to be saved
    :param query_name: Name of the query or file (used for the filename)
    :param output_dir: Directory where the Parquet file will be saved
    """
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Define the output file path
    output_path = os.path.join(output_dir, f"{query_name}.parquet")

    # Save the DataFrame as a Parquet file
    df.to_parquet(output_path)
    print(f"Saved query results to {output_path}")
