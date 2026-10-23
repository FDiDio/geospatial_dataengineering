import logging
import os
import threading

import h3
import pandas as pd

from utils.data_writer import write_parquet_atomic

logger = logging.getLogger(__name__)


def precompute_h3_index_lookup(df, resolution=5):
    """
    Precompute H3 geospatial indices as a lookup table.

    Args:
        df (pd.DataFrame): DataFrame containing latitude and longitude columns.
        resolution (int): H3 resolution for geospatial indexing.

    Returns:
        pd.DataFrame: A lookup table with unique coordinates and H3 indices.
    """
    logger.info("Precomputing H3 lookup table at resolution %d", resolution)

    unique_coords = df[["latitude", "longitude"]].drop_duplicates().reset_index(drop=True)
    unique_coords["h3_index"] = [
        h3.latlng_to_cell(lat, lon, resolution)
        for lat, lon in zip(unique_coords["latitude"].tolist(), unique_coords["longitude"].tolist(), strict=True)
    ]
    return unique_coords


def map_h3_indices_to_dataframe(df, h3_lookup):
    """
    Map precomputed H3 indices to the DataFrame based on latitude and longitude.

    Args:
        df (pd.DataFrame): DataFrame to map H3 indices onto.
        h3_lookup (pd.DataFrame): Lookup table containing precomputed H3 indices.

    Returns:
        pd.DataFrame: DataFrame with H3 indices mapped to it.
    """
    logger.info("Mapping H3 indices to the DataFrame")
    df_with_h3 = df.merge(h3_lookup, on=["latitude", "longitude"], how="left")
    return df_with_h3


def lookup_file_path(lookup_dir, resolution):
    """Return the path of the cached lookup table for the given resolution."""
    return os.path.join(lookup_dir, f"h3_lookup_res{resolution}.parquet")


def enrich_with_h3(df, resolution, lookup_dir, h3_lookup=None):
    """
    Add H3 indices to the DataFrame, reusing a lookup table whenever it covers the grid.

    The ERA5 grid is identical every day, so the lookup is computed once, cached on
    disk and reused by the following days and by later runs. It is recomputed only
    if some coordinates of the DataFrame are missing from it.

    Args:
        df (pd.DataFrame): DataFrame containing latitude and longitude columns.
        resolution (int): H3 resolution for geospatial indexing.
        lookup_dir (str): Directory where the lookup table is cached.
        h3_lookup (pd.DataFrame): Lookup table already in memory, if any.

    Returns:
        tuple: The DataFrame with H3 indices and the lookup table used.
    """
    path = lookup_file_path(lookup_dir, resolution)
    if h3_lookup is None and os.path.exists(path):
        logger.info("Loading cached H3 lookup table from %s", path)
        h3_lookup = pd.read_parquet(path)

    if h3_lookup is not None:
        df_with_h3 = map_h3_indices_to_dataframe(df, h3_lookup)
        if not df_with_h3["h3_index"].isna().any():
            return df_with_h3, h3_lookup
        logger.info("H3 lookup table does not cover the current grid, recomputing it")

    h3_lookup = precompute_h3_index_lookup(df, resolution)
    write_parquet_atomic(h3_lookup, path)
    return map_h3_indices_to_dataframe(df, h3_lookup), h3_lookup


class H3Enricher:
    """
    Thread-safe H3 enrichment, sharing one lookup table between the workers of a run.

    The lookup is read, mapped and validated without locking; loading or recomputing
    it is serialized, so concurrent workers never compute it twice.
    """

    def __init__(self, resolution: int, lookup_dir: str):
        self.resolution = resolution
        self.lookup_dir = lookup_dir
        self._lookup: pd.DataFrame | None = None
        self._lock = threading.Lock()

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        lookup = self._lookup
        if lookup is not None:
            df_with_h3 = map_h3_indices_to_dataframe(df, lookup)
            if not df_with_h3["h3_index"].isna().any():
                return df_with_h3

        with self._lock:
            df_with_h3, self._lookup = enrich_with_h3(df, self.resolution, self.lookup_dir, self._lookup)
        return df_with_h3
