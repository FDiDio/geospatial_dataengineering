import pandas as pd
import xarray as xr
import gcsfs
from datetime import datetime, timedelta

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
    delta = timedelta(days=1)
    dates = []

    while start <= end:
        dates.append((start.year, start.month, start.day))
        start += delta

    return dates

def extract_precipitation_data(bucket_path, year, month, day, variable_name, file_name):
    """
    Extract precipitation data from the Google Cloud Storage (GCS) bucket for a specific date.

    Args:
        bucket_path (str): GCS bucket path.
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
        variable_name (str): The variable name, e.g., "tp" for total precipitation.
        file_name (str): File name pattern to match.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted precipitation data.
    """
    print(f"Extracting precipitation data for {year}-{month:02d}-{day:02d}")
    fs = gcsfs.GCSFileSystem()
    path = f"{bucket_path}{year}/{month:02d}/{day:02d}/{variable_name}/{file_name}"
    files = fs.glob(path)

    if not files:
        raise FileNotFoundError(f"No files found for {year}-{month:02d}-{day:02d}.")

    combined_df = pd.DataFrame()
    for file in files:
        with fs.open(file, 'rb') as f:
            dataset = xr.open_dataset(f, engine='scipy')
            if 'tp' not in dataset.variables:
                continue
            df = dataset.to_dataframe().reset_index()
            df['tp'] = df['tp'].apply(lambda x: 0 if abs(x) < 1e-15 else x)  # Filter negligible values.
            combined_df = pd.concat([combined_df, df], ignore_index=True)

    if combined_df.empty:
        raise ValueError(f"No data found for {year}-{month:02d}-{day:02d}.")

    return combined_df