import logging
import time
import pandas as pd
import xarray as xr
import gcsfs
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def generate_date_range(start_date, end_date):
    """
    Generate a list of dates between the specified start and end date, inclusive.

    Args:
        start_date (str): Start date in the format YYYYMMDD.
        end_date (str): End date in the format YYYYMMDD.

    Returns:
        list: A list of tuples containing year, month, and day for each date in the range.
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}.")

    delta = timedelta(days=1)
    dates = []

    while start <= end:
        dates.append((start.year, start.month, start.day))
        start += delta

    return dates


def extract_variable_data(bucket_path, year, month, day, variable, file_name,
                          max_retries=3, retry_delay=2, fs=None):
    """
    Extract the data of an ERA5 variable from the Google Cloud Storage (GCS) bucket for a specific date.

    Transient errors are retried with exponential backoff; missing files are not.

    Args:
        bucket_path (str): GCS bucket path.
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
        variable (VariableSpec): Variable to extract, see utils.variables.
        file_name (str): File name pattern to match.
        max_retries (int): Number of attempts before giving up.
        retry_delay (float): Base delay in seconds between attempts.
        fs (fsspec.AbstractFileSystem): Filesystem to read from (defaults to GCS).

    Returns:
        pd.DataFrame: A DataFrame containing the extracted and cleaned data.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return _read_daily_files(bucket_path, year, month, day, variable, file_name, fs)
        except (FileNotFoundError, ValueError):
            raise
        except Exception as exc:
            if attempt == max_retries:
                raise
            wait = retry_delay * 2 ** (attempt - 1)
            logger.warning("Attempt %d/%d for %s %d-%02d-%02d failed (%s), retrying in %ss",
                           attempt, max_retries, variable.name, year, month, day, exc, wait)
            time.sleep(wait)


def _read_daily_files(bucket_path, year, month, day, variable, file_name, fs=None):
    logger.info("Extracting %s data for %d-%02d-%02d", variable.name, year, month, day)
    fs = fs or gcsfs.GCSFileSystem()
    path = f"{bucket_path}{year}/{month:02d}/{day:02d}/{variable.name}/{file_name}"
    files = fs.glob(path)

    if not files:
        raise FileNotFoundError(f"No {variable.name} files found for {year}-{month:02d}-{day:02d}.")

    frames = []
    for file in files:
        with fs.open(file, 'rb') as f, xr.open_dataset(f, engine='scipy') as dataset:
            if variable.short_name not in dataset.variables:
                continue
            df = dataset[[variable.short_name]].to_dataframe().reset_index()
            frames.append(variable.clean(df, variable.short_name))

    if not frames:
        raise ValueError(f"No {variable.name} data found for {year}-{month:02d}-{day:02d}.")

    return pd.concat(frames, ignore_index=True)
