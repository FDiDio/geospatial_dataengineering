import pyarrow.dataset as ds
import pandas as pd
import pyarrow as pa
from datetime import datetime
import os
from utils.common_utils import save_query_results_to_parquet

def run_queries_on_data(parquet_dir, output_query_dir):
    """
    Run sample queries on the precipitation data stored in Parquet files using PyArrow.

    Args:
        parquet_dir (str): Directory containing Parquet files.
        output_dir (str): Directory to save the query results.

    Returns:
        tuple: Filtered DataFrames based on time range, H3 index, and geospatial region.
    """
    print("Running sample queries on the Parquet files using PyArrow")

    # Load the Parquet dataset into PyArrow
    dataset = ds.dataset(parquet_dir, format="parquet")

    # Query 1: Filter by Time Range (2022-01-15 to 2022-03-31)
    filter_expr_time = (ds.field('time').cast(pa.timestamp('s')) >= pd.Timestamp('2022-01-15').to_pydatetime()) & \
                       (ds.field('time').cast(pa.timestamp('s')) <= pd.Timestamp('2022-03-31').to_pydatetime())
    table_filtered_time = dataset.to_table(filter=filter_expr_time)
    df_filtered_time = table_filtered_time.to_pandas()
    print(f"Filtered data by time range (2022-01-15 to 2022-03-31): Results:\n{df_filtered_time.head()}")
    save_query_results_to_parquet(df_filtered_time, "time_range", output_query_dir)

    # Query 2: Filter by Geospatial Region (Latitude: 40.0 to 42.0, Longitude: 73.0 to 75.0)
    filter_expr_geo_time = (ds.field('h3_index') == '85df6c57fffffff') & \
                           (ds.field('time').cast(pa.timestamp('s')) >= pd.Timestamp('2023-05-01').to_pydatetime()) & \
                           (ds.field('time').cast(pa.timestamp('s')) <= pd.Timestamp('2023-06-30').to_pydatetime())
    table_filtered_geo_time = dataset.to_table(filter=filter_expr_geo_time)
    df_filtered_geo_time = table_filtered_geo_time.to_pandas()
    print(f"Filtered data by time range and geospatial region (Lat: 40.0-42.0, Long: 73.0-75.0): Results:\n{df_filtered_geo_time.head()}")
    save_query_results_to_parquet(df_filtered_geo_time, "geo_time_range", output_query_dir)

    # Query 3: Filter by Specific H3 index
    filter_expr_h3 = ds.field('h3_index') == '85032097fffffff'
    table_filtered_h3 = dataset.to_table(filter=filter_expr_h3)
    df_filtered_h3 = table_filtered_h3.to_pandas()
    print(f"Filtered data by H3 index: Results:\n{df_filtered_h3.head()}")
    save_query_results_to_parquet(df_filtered_h3, "h3_index", output_query_dir)

    # Query 4: Sum of Total Precipitation around Zurich area during April 2022
    filter_expr_zurich_april = (ds.field('latitude') == 47.25) & \
                               (ds.field('longitude') == 8.5) & \
                               (ds.field('time').cast(pa.timestamp('s')) >= pd.Timestamp('2022-04-01').to_pydatetime()) & \
                               (ds.field('time').cast(pa.timestamp('s')) <= pd.Timestamp('2022-04-30').to_pydatetime())
    table_zurich_april = dataset.to_table(filter=filter_expr_zurich_april)
    df_zurich_april = table_zurich_april.to_pandas()
    total_precipitation_zurich = df_zurich_april['tp'].sum()
    print(f"Total precipitation in Zurich for April 2022: {total_precipitation_zurich*1000} mm")

    # Save the results of the last query separately if necessary
    # No need to save total precipitation to parquet, it's a scalar value.
    
    return df_filtered_time, df_filtered_geo_time, df_filtered_h3, df_zurich_april

