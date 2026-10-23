import numpy as np
import pandas as pd
import pytest


def make_era5_df(days, column="tp", value=0.001, latitudes=(47.25, 47.5), longitudes=(8.5, 8.75)):
    """Build an hourly DataFrame shaped like the ERA5 extraction output of a variable."""
    times = pd.date_range(days[0], pd.Timestamp(days[-1]) + pd.Timedelta(hours=23), freq="h")
    index = pd.MultiIndex.from_product(
        [times, np.array(latitudes, dtype="float32"), np.array(longitudes, dtype="float32")],
        names=["time", "latitude", "longitude"],
    )
    df = index.to_frame(index=False)
    df[column] = np.float32(value)
    return df


def make_precipitation_df(days, tp=0.001):
    return make_era5_df(days, "tp", tp)


def make_temperature_df(days, kelvin=283.15):
    """Temperature varying with the hour of the day: kelvin - 5 at midnight, kelvin + 6 at 23:00."""
    df = make_era5_df(days, "t2m", kelvin)
    df["t2m"] = (df["t2m"] + df["time"].dt.hour * 0.5 - 5).astype("float32")
    return df


def to_data_array(df, column):
    """Turn an ERA5-shaped DataFrame back into the (time, latitude, longitude) grid of a source."""
    return df.set_index(["time", "latitude", "longitude"])[column].to_xarray()


# Synthetic values of the ERA5 fields, by folder name
FIELD_MAKERS = {
    "total_precipitation": make_precipitation_df,
    "2m_temperature": make_temperature_df,
    "2m_dewpoint_temperature": lambda days: make_era5_df(days, "d2m", 278.15),
    "10m_u_component_of_wind": lambda days: make_era5_df(days, "u10", 3.0),
    "10m_v_component_of_wind": lambda days: make_era5_df(days, "v10", -4.0),
}


class FakeSource:
    """In-memory source of synthetic ERA5 fields, recording the reads; missing holds (field, (y, m, d)) pairs."""

    def __init__(self, missing=()):
        self.missing = set(missing)
        self.calls = []

    def read(self, field, year, month, day):
        date = (year, month, day)
        self.calls.append((field.name, date))
        if (field.name, date) in self.missing:
            raise FileNotFoundError(f"No {field.name} files found for {year}-{month:02d}-{day:02d}.")
        df = FIELD_MAKERS[field.name]([f"{year}-{month:02d}-{day:02d}"])
        return to_data_array(df, field.short_name)


@pytest.fixture
def precipitation_df():
    return make_precipitation_df(["2022-04-30"])
