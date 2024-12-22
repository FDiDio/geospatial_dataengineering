# Geospatial Data Engineering

The implementation follows a **daily iterative** approach to process the precipitation data, considering both the nature of the source files and memory constraints. 

The files, stored in Google Cloud Storage, are downloaded with a daily granularity based on the specified date interval. This iterative processing minimizes memory consumption, as processing large datasets (in this case a full year of data, potentially spanning multiple gigabytes) could result in memory bottlenecks.

A command-line argument specifies a **configuration file** that parametrizes variables within the application. This decouples configuration from code, enhancing flexibility, maintainability, and ease of deployment without modifying the codebase.

Two additional arguments control optionally the date range for processing, providing flexibility to handle data for different periods, with **2022** set as the default year.


## Pipeline Steps
**1. File Download and Data Conversion:**
- The process starts by automatically downloading the daily NetCDF file from Google Cloud Storage ERA5 dataset.
- The file is then converted into a Pandas DataFrame. The choice of Pandas is driven by its native support for multidimensional data through **xarray** library, which is particularly suitable for handling NetCDF files in this situation.

- In a distributed context, **Spark** could be used alternatively to parallelize file downloads and processing across multiple nodes, reducing extraction time and enabling faster data transformations and aggregations, and improving scalability and performance.

**2. Geospatial Index Generation:**

- I opted for an **H3** hierarchical geospatial index to manage the data's geographical structure (latitudes and longitudes). 
- The H3 index is stored in an external **lookup table** for efficient enrichment of daily dataframes, significantly improving performance during subsequent processing steps.

**3. Data Enrichment and Storage:**

- The processed data is enriched with the **H3 index** and subsequently written to a **Parquet** file.

**4. Data Querying:**

- After processing all daily data, sample queries are performed on the generated Parquet files to verify the integrity and correctness of the data, as well as to assess the performance of the file system.
- The chosen tool is **PyArrow**, given its efficiency for querying large datasets in this context due to its optimized columnar storage (Parquet format), in-memory processing, and integration with Pandas. It provides low-latency filtering and aggregation. 
- **Spark** is suitable (also in this case) for massive distributed data processing, but PyArrow results usually more efficient for local query execution on medium sized data.

## Requirements

Before running the script, ensure you have the following installed:

- **Python 3.x**: A version of Python 3 (preferably 3.6+).
- **pip**: The Python package installer.

### Install Required Python Libraries

You can install the necessary dependencies using the following command:

```bash
pip install argparse pyyaml pandas gcsfs xarray h3 pyarrow
```

## Configuration file 
The script requires a YAML configuration file (config.yml) that contains necessary information such as the GCS bucket path, output path, and other parameters.



- **bucket_path**: The path to the GCS bucket containing the data files.

- **output_path**: The local directory where the processed Parquet files will be stored.

- **query_output_path**: The local directory where the output of the sample queries will be stored.

- **h3_resolution**: The resolution of the H3 geospatial indexing (integer value).

- **variable_name**: The name of the variable in the dataset (e.g., total_precipitation).

- **file_name**: The name of the data files in the dataset (e.g., data_file.nc).



## Running the Script
Command-Line Arguments
To run the script, use the following command format:

```bash
python src/main_script.py --config ../config/config.yaml --start_date YYYYMMDD --end_date YYYYMMDD
```
- --**config**: Path to the YAML configuration file. (optional, default: ../config/config.yaml)

- --**start_date**: The start date of the data processing in YYYYMMDD format (optional, default: 20220101).

- --**end_date**: The end date of the data processing in YYYYMMDD format (optional, default: 20221231).

The main script is inside the **src**, together with other modules in the **utils** folder.


```bash
python src/main_script.py --config ../config/config.yaml --start_date 20220101 --end_date 20221231
```

This command will process precipitation data for the year 2022, from January 1st to December 31st, based on the configuration in config.yml.



## Testing the Application

To test the functionality of the script, verify that the output Parquet files are created in the specified output path. Additionally, some sample queries will be run to verify the filtering capabilities. These queries can be customized based on the dataset and specific requirements, and they will produce a Parquet for further analysis.

Here’s a sample approach to test and check the functionality:

**1. Check of Output Parquet Files**

After running the script, verify that the output Parquet files are generated in the specified output directory. The `output_dir` should be set in your configuration file (`config.yaml`), and by default, it saves files to the `output_data/total_precipitation` directory.

#### Steps:
- Run the script as shown below:
    ```bash
    python main.py --start_date 20220101 --end_date 20220131 --config_file config.yaml
    ```
  It is possible to compute a smaller interval for fast testing purposes.
  
- Check the `output/total_precipitation` directory (or the path specified in your configuration file) for Parquet files. The files should be named according to the date range (year/month/day) and the specific query results.



**2. Sample Queries to Verify Filtering and Check the Output**

After the data is processed, the script runs sample queries on the resulting Parquet files. These queries filter the data based on time ranges, geospatial regions, and H3 indices. You can adjust the filters according to your needs by using PyArrow's SQL-like querying capabilities like the followings.

#### Key PyArrow Functions for Querying:

- Filtering:
   - Filters allow you to apply conditions on fields using comparison operators (e.g., `>=`, `<=`, `==`) or logical operators (e.g., `&`, `|`).
   - Example (time range filter):
     ```python
     filter_expr = (time_field >= pd.Timestamp('2022-01-15').to_pydatetime()) & \
                   (time_field <= pd.Timestamp('2022-03-31').to_pydatetime())
     ```


- Aggregation:
   - After filtering the dataset, you can perform aggregation operations such as `sum`, `mean`, etc.
   - Example (sum of precipitation):
     ```python
     total_precipitation = df_filtered['tp'].sum()
     ```

- Casting Fields:
   - You can cast fields to the appropriate types (e.g., `timestamp`, `string`, `int`) to perform comparisons.
   - Example:
     ```python
     time_cast = time_field.cast(pa.timestamp('s'))
     ```



**3. Saving Query Results**

Query results are saved as Parquet files in the specified output directory. After each query, use the `save_query_results_to_parquet` function to save the results.

#### Example of Saving Results:
```python
save_query_results_to_parquet(df_filtered_time, "time_range", output_dir)
