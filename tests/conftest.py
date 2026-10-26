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


def fake_archive_fetch(latitude, longitude, start, end, source=None):
    """
    Weather of a place as the Open-Meteo archive gives it: high 25 °C + day of the month, 2 mm on even days,
    a west wind of 10 km/h + the day of the month modulo 5, gusts of 30 km/h + the day of the month.
    """
    from dashboard.archive import Weather

    days = pd.date_range(start, end, freq="D")
    n = days.day.to_numpy()
    daily = pd.DataFrame(
        {
            "date": days,
            "temperature_max_c": 25.0 + n,
            "temperature_mean_c": 20.0 + n,
            "temperature_min_c": 15.0 + n,
            "precipitation_mm": np.where(n % 2 == 0, 2.0, 0.0),
            "wind_speed_mean_kmh": 10.0 + n % 5,
            "wind_speed_max_kmh": 20.0 + n % 5,
            "wind_gusts_max_kmh": 30.0 + n,
            "wind_direction_deg": 270.0,
            "humidity_mean_pct": 60.0 + n % 10,
            "humidity_min_pct": 40.0,
            "humidity_max_pct": 90.0,
            "cloud_cover_mean_pct": 50.0,
            "sunshine_h": 6.0 + n % 3,
            "weather_code": np.where(n % 2 == 0, 61, 1),  # light rain on the wet even days, mainly clear on the odd
        }
    )
    hours = pd.date_range(start, pd.Timestamp(end) + pd.Timedelta(hours=23), freq="h", tz="Europe/Zurich")
    hourly = pd.DataFrame(
        {
            "time": hours,
            "temperature_c": 15.0 + hours.hour * 0.5,
            "precipitation_mm": 0.1,
            "wind_speed_kmh": 12.0,
            "wind_direction_deg": 225.0,
            "wind_gusts_kmh": 25.0,
            "humidity_pct": 70.0,
            "cloud_cover_pct": 40.0,
            "sunshine_s": np.where((hours.hour >= 8) & (hours.hour < 18), 1800.0, 0.0),
            "weather_code": 2,
            "is_day": ((hours.hour >= 7) & (hours.hour < 19)).astype(int),
        }
    )
    name = "ERA5" if source is None else source.name
    point = (round(latitude * 4) / 4, round(longitude * 4) / 4, 400.0)
    return Weather(*point, "Europe/Zurich", daily.assign(source=name), hourly.assign(source=name), points={name: point})


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
