import logging
import re
import time
from datetime import UTC, datetime, timedelta

import pandas as pd
import xarray as xr

from utils.sources import Source
from utils.variables import VariableSpec

logger = logging.getLogger(__name__)


def parse_date(text):
    """
    Parse a date in the format YYYYMMDD, or relative to the current UTC day: "today", "today-6".

    Relative dates let a scheduled run always target the latest days published by ERA5.
    """
    relative = re.fullmatch(r"today(?:-(\d+))?", text)
    if relative:
        today = datetime.now(UTC).replace(tzinfo=None, hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=int(relative.group(1) or 0))
    return datetime.strptime(text, "%Y%m%d")


def generate_date_range(start_date, end_date):
    """
    Generate a list of dates between the specified start and end date, inclusive.

    Args:
        start_date (str): Start date in the format YYYYMMDD, or relative like "today-10".
        end_date (str): End date in the format YYYYMMDD, or relative like "today-6".

    Returns:
        list: A list of tuples containing year, month, and day for each date in the range.
    """
    start = parse_date(start_date)
    end = parse_date(end_date)
    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}.")

    delta = timedelta(days=1)
    dates = []

    while start <= end:
        dates.append((start.year, start.month, start.day))
        start += delta

    return dates


def extract_variable_data(
    source: Source,
    year: int,
    month: int,
    day: int,
    variable: VariableSpec,
    max_retries: int = 3,
    retry_delay: float = 2,
) -> pd.DataFrame:
    """
    Extract the hourly data of an ERA5 variable for a specific date.

    A derived variable is computed from its source fields. Transient errors are
    retried with exponential backoff; missing data is not.

    Args:
        source (Source): Where the ERA5 fields are read from, see utils.sources.
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
        variable (VariableSpec): Variable to extract, see utils.variables.
        max_retries (int): Number of attempts before giving up.
        retry_delay (float): Base delay in seconds between attempts.

    Returns:
        pd.DataFrame: Columns time, latitude, longitude and the variable, cleaned.
    """
    for attempt in range(1, max_retries + 1):
        try:
            return _read_variable_day(source, year, month, day, variable)
        except (FileNotFoundError, ValueError):
            raise
        except Exception as exc:
            if attempt == max_retries:
                raise
            wait = retry_delay * 2 ** (attempt - 1)
            logger.warning(
                "Attempt %d/%d for %s %d-%02d-%02d failed (%s), retrying in %ss",
                attempt, max_retries, variable.name, year, month, day, exc, wait,
            )  # fmt: skip
            time.sleep(wait)
    raise RuntimeError("max_retries must be at least 1")


def _read_variable_day(source: Source, year: int, month: int, day: int, variable: VariableSpec) -> pd.DataFrame:
    logger.info("Extracting %s data for %d-%02d-%02d", variable.name, year, month, day)
    inputs = {field.short_name: source.read(field, year, month, day) for field in variable.fields}
    data = variable.derive(xr.Dataset(inputs)) if variable.derive else inputs[variable.short_name]

    # ERA5 values are packed on 16 bits: float32 keeps their precision with half the memory of float64
    df = data.astype("float32").rename(variable.short_name).to_dataframe().reset_index()
    return variable.clean(df, variable.short_name)
