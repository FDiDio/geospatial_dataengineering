import numpy as np
import pandas as pd
import pytest


def make_era5_df(days, column="tp", value=0.001, latitudes=(47.25, 47.5), longitudes=(8.5, 8.75)):
    """Build an hourly DataFrame shaped like the ERA5 extraction output of a variable."""
    times = pd.date_range(days[0], pd.Timestamp(days[-1]) + pd.Timedelta(hours=23), freq="H")
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


@pytest.fixture
def precipitation_df():
    return make_precipitation_df(["2022-04-30"])
