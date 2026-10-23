import logging
import os
from collections.abc import Sequence
from typing import Any, Protocol

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from utils.filters import Condition, to_arrow_expression

logger = logging.getLogger(__name__)

SINKS = ("parquet", "iceberg")

# Hive-style partitioning written by daily_output_path
PARTITIONING = ds.partitioning(pa.schema([("year", pa.int16()), ("month", pa.int8())]), flavor="hive")


def write_parquet_atomic(df, output_filename):
    """
    Write the DataFrame to a Parquet file, replacing it only once the write is complete.

    The data is first written to a hidden temporary file in the same directory, which
    PyArrow datasets ignore, so an interrupted run never leaves a truncated file behind.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_filename (str): Destination Parquet file.
    """
    directory, name = os.path.split(output_filename)
    os.makedirs(directory, exist_ok=True)
    tmp_filename = os.path.join(directory, f".{name}.tmp")
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), tmp_filename)
    os.replace(tmp_filename, output_filename)


def dataset_path(output_path, variable_name):
    """Return the root directory of the dataset of a variable."""
    return os.path.join(output_path, variable_name)


def daily_output_path(output_path, variable_name, year, month, day):
    """
    Return the Parquet file of a single variable and day, inside its Hive-style partition.

    Args:
        output_path (str): Root output directory, containing one dataset per variable.
        variable_name (str): Name of the variable, e.g. "total_precipitation".
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.

    Returns:
        str: Path like <output_path>/total_precipitation/year=2022/month=01/2022-01-01_total_precipitation.parquet
    """
    return os.path.join(
        dataset_path(output_path, variable_name),
        f"year={year}",
        f"month={month:02d}",
        f"{year}-{month:02d}-{day:02d}_{variable_name}.parquet",
    )


def save_data_to_parquet(df, output_path, variable_name, year, month, day):
    """
    Save the DataFrame to a Parquet file in the dataset of its variable.

    Args:
        df (pd.DataFrame): DataFrame to save.
        output_path (str): Root output directory, containing one dataset per variable.
        variable_name (str): Name of the variable, e.g. "total_precipitation".
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
    """
    logger.info("Saving %s data for %d-%02d-%02d to Parquet", variable_name, year, month, day)
    write_parquet_atomic(df, daily_output_path(output_path, variable_name, year, month, day))


def save_query_results_to_parquet(df, query_name, output_dir):
    """
    Save filtered query results to a Parquet file in the specified output directory.

    Args:
        df (pd.DataFrame): Filtered query results to save.
        query_name (str): Identifier for the query (used in file naming).
        output_dir (str): Directory to save the query results.
    """
    output_filename = os.path.join(output_dir, f"{query_name}.parquet")
    write_parquet_atomic(df, output_filename)
    logger.info("Saved query results to %s", output_filename)


class Sink(Protocol):
    """Where the processed variable-days are stored and queried."""

    def exists(self, variable_name: str, year: int, month: int, day: int) -> bool:
        """Return True if the variable-day has already been written."""
        ...

    def write(self, df: pd.DataFrame, variable_name: str, year: int, month: int, day: int) -> None:
        """Write a variable-day, replacing it if it exists."""
        ...

    def read(
        self, variable_name: str, conditions: Sequence[Condition], columns: list[str] | None = None
    ) -> pa.Table | None:
        """Return the rows matching all the conditions, or None if the variable has no data yet."""
        ...


class ParquetSink:
    """Hive-partitioned Parquet datasets, one per variable: <output_path>/<variable>/year=YYYY/month=MM."""

    def __init__(self, output_path: str):
        self.output_path = output_path

    def exists(self, variable_name: str, year: int, month: int, day: int) -> bool:
        return os.path.exists(daily_output_path(self.output_path, variable_name, year, month, day))

    def write(self, df: pd.DataFrame, variable_name: str, year: int, month: int, day: int) -> None:
        save_data_to_parquet(df, self.output_path, variable_name, year, month, day)

    def read(
        self, variable_name: str, conditions: Sequence[Condition], columns: list[str] | None = None
    ) -> pa.Table | None:
        directory = dataset_path(self.output_path, variable_name)
        if not os.path.isdir(directory):
            return None
        dataset = ds.dataset(directory, format="parquet", partitioning=PARTITIONING)
        return dataset.to_table(filter=to_arrow_expression(conditions), columns=columns)


def build_sink(config: dict[str, Any]) -> Sink:
    """Return the sink selected by the 'sink' configuration key."""
    if config["sink"] == "iceberg":
        # pyiceberg is imported only when the Iceberg sink is used
        from utils.iceberg_sink import IcebergSink

        return IcebergSink.from_config(config)
    return ParquetSink(config["output_path"])
