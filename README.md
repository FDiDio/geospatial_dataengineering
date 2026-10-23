# Geospatial Data Engineering

The pipeline processes hourly ERA5 reanalysis data, currently **total precipitation** and **2m temperature**, from the public ERA5 bucket on Google Cloud Storage.

The implementation follows a **daily iterative** approach to process the data, considering both the nature of the source files and memory constraints.

The files, stored in Google Cloud Storage, are downloaded with a daily granularity based on the specified date interval. This iterative processing minimizes memory consumption, as processing large datasets (in this case a full year of data, potentially spanning multiple gigabytes) could result in memory bottlenecks.

A command-line argument specifies a **configuration file** that parametrizes variables within the application, including the sample queries. This decouples configuration from code, enhancing flexibility, maintainability, and ease of deployment without modifying the codebase.

Two additional arguments control optionally the date range for processing, providing flexibility to handle data for different periods, with **2022** set as the default year.


## Pipeline Steps
**1. File Download and Data Conversion:**
- The process starts by automatically downloading the daily NetCDF file of each variable from Google Cloud Storage ERA5 dataset.
- The file is then converted into a Pandas DataFrame. The choice of Pandas is driven by its native support for multidimensional data through **xarray** library, which is particularly suitable for handling NetCDF files in this situation.
- Each variable is cleaned with its own logic (see [Variables](#variables)).
- Transient download errors are retried with exponential backoff. A variable-day that still fails is logged and skipped, so it does not stop the rest of the run.

- In a distributed context, **Spark** could be used alternatively to parallelize file downloads and processing across multiple nodes, reducing extraction time and enabling faster data transformations and aggregations, and improving scalability and performance.

**2. Geospatial Index Generation:**

- I opted for an **H3** hierarchical geospatial index to manage the data's geographical structure (latitudes and longitudes).
- The ERA5 grid is the same every day and for every variable, so the H3 index is computed once and stored in an external **lookup table** (`h3_lookup_path`). Following days, other variables and later runs reuse it, and it is recomputed only if the grid changes.

**3. Data Enrichment and Storage:**

- The processed data is enriched with the **H3 index** and subsequently written to a **Parquet** file, one per variable and day. Each variable has its own dataset, in Hive-style partitions (`<variable>/year=YYYY/month=MM`), so variables can be added or reprocessed independently.
- Files are written to a temporary file first and then renamed, so an interrupted run never leaves a truncated file behind. Days already written are skipped on the next run, which makes it possible to resume a long run.

**4. Data Querying:**

- After processing all daily data, the queries defined in the configuration file are performed on the generated Parquet files to verify the integrity and correctness of the data, as well as to assess the performance of the file system.
- The chosen tool is **PyArrow**, given its efficiency for querying large datasets in this context due to its optimized columnar storage (Parquet format), in-memory processing, and integration with Pandas. It provides low-latency filtering and aggregation.
- **Spark** is suitable (also in this case) for massive distributed data processing, but PyArrow results usually more efficient for local query execution on medium sized data.

## Requirements

- **Python 3.9 - 3.11**
- **pip**: The Python package installer.

### Install Required Python Libraries

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows (on Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt
```

The ERA5 bucket is public: no Google Cloud credentials are needed.

## Configuration file
The script reads a YAML configuration file (default: [config/config.yaml](config/config.yaml)). Relative paths are resolved against the directory containing the configuration file, so the script can be launched from any working directory.

- **bucket_path**: The path to the GCS bucket containing the data files.

- **output_path**: The local root directory of the processed Parquet files, containing one dataset per variable.

- **query_output_path**: The local directory where the output of the queries will be stored. It must not be inside a variable dataset, otherwise the results would be read back as data.

- **h3_lookup_path**: The local directory where the H3 lookup table is cached.

- **h3_resolution**: The resolution of the H3 geospatial indexing (integer value).

- **variables**: The variables to process, among the supported ones (see [Variables](#variables)).

- **file_name**: The name of the data files in the dataset (e.g., surface.nc).

- **max_retries**: Number of download attempts for each day (optional, default: 3).

- **queries**: The list of queries to run after processing (optional, see [Queries](#queries)).

## Variables

ERA5 variables differ in nature, so each one has its own logic, defined in [src/utils/variables.py](src/utils/variables.py):

| Variable | Column | Stored unit | Cleaning | Aggregates (reported unit) |
|---|---|---|---|---|
| `total_precipitation` | `tp` | m, hourly accumulation | Values below 1e-15 set to 0 | `sum` (mm) |
| `2m_temperature` | `t2m` | K, instantaneous | Values outside 150-350 K masked as missing | `mean`, `min`, `max` (°C) |

The Parquet files keep the ERA5 units; conversions are applied only to the aggregated values. A sum of temperatures is meaningless and is rejected.

To add a variable, add a `VariableSpec` to the registry with the folder name used in the bucket (e.g. `10m_u_component_of_wind`), the variable name inside the NetCDF file, its cleaning and its aggregations, then list it in `variables`.


## Running the Script

```bash
python src/main_script.py --config config/config.yaml --start_date YYYYMMDD --end_date YYYYMMDD
```

- **--config** (alias **--config_file**): Path to the YAML configuration file (optional, default: `config/config.yaml` in the project).

- **--start_date**: The start date of the data processing in YYYYMMDD format (optional, default: 20220101).

- **--end_date**: The end date of the data processing in YYYYMMDD format (optional, default: 20221231).

- **--variables**: The variables to process, e.g. `--variables 2m_temperature` (optional, default: the `variables` of the configuration file).

- **--overwrite**: Reprocess the days whose Parquet file already exists (by default they are skipped).

- **--skip_queries**: Do not run the queries at the end.

- **--log_level**: Logging verbosity, one of DEBUG, INFO, WARNING, ERROR (default: INFO).

The main script is inside the **src**, together with other modules in the **utils** folder.

```bash
python src/main_script.py --start_date 20220101 --end_date 20221231
```

This command processes the configured variables for the year 2022, from January 1st to December 31st. Each variable-day takes around half a minute, most of it spent downloading the ~50 MB daily file. The exit code is `1` if at least one variable-day failed: the failures are listed at the end of the log and are retried by simply running the same command again.

The output looks like this:

```
output/
├── total_precipitation/
│   └── year=2022/
│       └── month=04/
│           └── 2022-04-30_total_precipitation.parquet
├── 2m_temperature/
│   └── year=2022/
│       └── month=04/
│           └── 2022-04-30_2m_temperature.parquet
├── h3_lookup/
│   └── h3_lookup_res5.parquet
└── query_results/
    ├── time_range.parquet
    └── ...
```

## Running with Docker

The image contains the pipeline and the default configuration; the output is written to the mounted `./output` directory.

```bash
docker compose build
docker compose run --rm pipeline --start_date 20220430 --end_date 20220430
docker compose run --rm tests
```

Any command-line argument of the script can be passed after `pipeline`. To use a different configuration, mount it and pass its path, e.g. `-v ./my_config.yaml:/app/config/my_config.yaml` and `--config config/my_config.yaml`.

## Queries

Queries are defined in the `queries` section of the configuration file. Each query has a `name`, the `variable` it targets and any combination of the following filters:

| Key | Description |
|---|---|
| `start` / `end` | Date range in `YYYY-MM-DD` format, both days included (every hour of the end day is kept) |
| `h3_index` | H3 cell identifier |
| `bbox` | `{lat_min, lat_max, lon_min, lon_max}`, with longitudes in the ERA5 0-360 convention |
| `point` | `{latitude, longitude, tolerance}`: grid points within `tolerance` degrees (default 0.01) |
| `aggregate` | One or a list of the aggregates supported by the variable: the values are logged in the reported unit instead of saving the rows |

Queries without `aggregate` save their rows to `<query_output_path>/<name>.parquet`. Unknown keys and aggregates not supported by the variable raise an error, a query returning no rows logs a warning, and queries on a variable not processed yet are skipped.

Example:

```yaml
queries:
  - name: zurich_april_total
    variable: total_precipitation
    point: {latitude: 47.25, longitude: 8.5, tolerance: 0.01}
    start: "2022-04-01"
    end: "2022-04-30"
    aggregate: sum

  - name: zurich_april_temperature
    variable: 2m_temperature
    point: {latitude: 47.25, longitude: 8.5, tolerance: 0.01}
    start: "2022-04-01"
    end: "2022-04-30"
    aggregate: [mean, min, max]
```

The partitioned dataset can also be queried directly with PyArrow:

```python
import pyarrow.dataset as ds
from utils.query_runner import PARTITIONING

dataset = ds.dataset("output/total_precipitation", format="parquet", partitioning=PARTITIONING)
df = dataset.to_table(filter=(ds.field("month") == 4) & (ds.field("h3_index") == "85032097fffffff")).to_pandas()
```

## Testing

The tests use synthetic data and an in-memory filesystem, so they do not download anything from GCS:

```bash
pip install -r requirements-dev.txt
python -m pytest
```

For a quick end-to-end check against the real bucket, process a single day:

```bash
python src/main_script.py --start_date 20220430 --end_date 20220430
```
