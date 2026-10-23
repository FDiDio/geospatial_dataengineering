import asyncio
import io
import logging
import sys
import threading
from typing import Any, Protocol

import gcsfs
import pandas as pd
import xarray as xr

from utils.variables import SourceField

logger = logging.getLogger(__name__)

SOURCES = ("netcdf", "zarr")


class Source(Protocol):
    """Where the hourly ERA5 fields are read from."""

    def read(self, field: SourceField, year: int, month: int, day: int) -> xr.DataArray:
        """Return the 24 hourly grids of a field for a day, named after field.short_name."""
        ...

    def available(self, field: SourceField, year: int, month: int, day: int) -> bool:
        """Return True if the field has been published for the day."""
        ...


class NetCDFSource:
    """
    Daily NetCDF files of the raw ARCO-ERA5 bucket, one per field and day.

    Each file (~50 MB) is downloaded with a single request and decoded in memory:
    decoding while streaming from GCS issues many small range requests and is ~4x slower.
    """

    def __init__(self, bucket_path: str, file_name: str, fs=None, storage_options: dict[str, Any] | None = None):
        self.bucket_path = bucket_path
        self.file_name = file_name
        # The ERA5 bucket is public: anonymous access skips the slow lookup of Google credentials
        self.fs = fs or gcsfs.GCSFileSystem(**(storage_options or {}))

    def path(self, field: SourceField, year: int, month: int, day: int) -> str:
        file_name = self.file_name.format(year=year, month=month, day=day)
        return f"{self.bucket_path}{year}/{month:02d}/{day:02d}/{field.name}/{file_name}"

    def available(self, field: SourceField, year: int, month: int, day: int) -> bool:
        return bool(self.fs.glob(self.path(field, year, month, day)))

    def read(self, field: SourceField, year: int, month: int, day: int) -> xr.DataArray:
        files = self.fs.glob(self.path(field, year, month, day))
        if not files:
            raise FileNotFoundError(f"No {field.name} files found for {year}-{month:02d}-{day:02d}.")

        arrays = []
        for file in files:
            with xr.open_dataset(io.BytesIO(self.fs.cat_file(file)), engine="scipy") as dataset:
                if field.short_name in dataset.variables:
                    arrays.append(dataset[field.short_name].load())

        if not arrays:
            raise ValueError(f"No {field.name} data found for {year}-{month:02d}-{day:02d}.")
        return arrays[0] if len(arrays) == 1 else xr.concat(arrays, dim="time")


class ZarrSource:
    """
    Analysis-ready ARCO-ERA5 Zarr store: all the fields on the hourly 0.25° grid, one chunk per hour.

    Only the 24 chunks of the requested field and day are fetched, concurrently.
    The store is opened lazily, once, and shared between threads.
    """

    def __init__(self, store: str, storage_options: dict[str, Any] | None = None):
        self.store = store
        self.storage_options = storage_options or {}
        if sys.platform == "win32":
            # With the default Proactor loop, closing the zarr event loop at exit waits forever on the GCS sockets
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        self._dataset: xr.Dataset | None = None
        self._lock = threading.Lock()

    @property
    def dataset(self) -> xr.Dataset:
        with self._lock:
            if self._dataset is None:
                logger.info("Opening Zarr store %s", self.store)
                # Storage options apply to remote stores (gs://...) only
                options = self.storage_options if "://" in self.store else None
                self._dataset = xr.open_zarr(self.store, chunks=None, storage_options=options)
            return self._dataset

    def available(self, field: SourceField, year: int, month: int, day: int) -> bool:
        last_day = self.dataset.attrs.get("valid_time_stop")
        return field.name in self.dataset.data_vars and (
            not last_day or pd.Timestamp(year=year, month=month, day=day) <= pd.Timestamp(last_day)
        )

    def read(self, field: SourceField, year: int, month: int, day: int) -> xr.DataArray:
        dataset = self.dataset
        start = pd.Timestamp(year=year, month=month, day=day)
        # The time axis of the store extends beyond the data already published
        last_day = dataset.attrs.get("valid_time_stop")
        if last_day and start > pd.Timestamp(last_day):
            raise FileNotFoundError(f"No {field.name} data for {start.date()}: the store ends on {last_day}.")
        if field.name not in dataset.data_vars:
            raise ValueError(f"No {field.name} data found in {self.store}.")

        data = dataset[field.name].sel(time=slice(start, start + pd.Timedelta(hours=23)))
        if data.sizes["time"] == 0:
            raise FileNotFoundError(f"No {field.name} data found for {start.date()}.")
        return data.load().rename(field.short_name)


def build_source(config: dict[str, Any]) -> Source:
    """Return the source selected by the 'source' configuration key."""
    if config["source"] == "zarr":
        return ZarrSource(config["zarr_store"], config["storage_options"])
    return NetCDFSource(config["bucket_path"], config["file_name"], storage_options=config["storage_options"])
