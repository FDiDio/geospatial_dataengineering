"""
How warm or cold the weather feels, as weather apps show it: the "feels like" of the US National Weather Service and
Environment Canada, from the air temperature, humidity and wind of each hour. In the cold, the wind chill, which the
wind lowers (Environment Canada and the NWS since 2001: at 10 °C or less, with more than 4.8 km/h of wind at 10 m);
in the heat, the heat index, which the humidity raises (NWS: from 26.7 °C, 80 °F, Rothfusz's regression with its
adjustments); in between, the air temperature itself.

The thermal comfort indices that also weigh the sunlight, as UTCI does, measure the strain of the body instead: by day
they run 5 to 8 °C above the air even under clouds, nothing like the feels-like of a forecast.
"""

import numpy as np
import pandas as pd

# The wind chill applies at this temperature or below, with more wind than this at 10 m; the heat index from this one
WIND_CHILL_C = 10.0
WIND_CHILL_KMH = 4.8
HEAT_INDEX_C = 26.7


def wind_chill(temperature_c: np.ndarray, wind_kmh: np.ndarray) -> np.ndarray:
    """The wind chill index of Environment Canada and the NWS, in °C, of the wind at 10 m in km/h."""
    wind = wind_kmh**0.16
    return 13.12 + 0.6215 * temperature_c - 11.37 * wind + 0.3965 * temperature_c * wind


def heat_index(temperature_c: np.ndarray, humidity_pct: np.ndarray) -> np.ndarray:
    """
    The heat index of the NWS, in °C: Steadman's simple formula where it stays below 80 °F, else Rothfusz's regression
    with its adjustments for dry and for humid air (https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml).
    """
    t, rh = temperature_c * 9 / 5 + 32, humidity_pct
    simple = 0.5 * (t + 61.0 + (t - 68.0) * 1.2 + rh * 0.094)
    regression = (
        -42.379
        + 2.04901523 * t
        + 10.14333127 * rh
        - 0.22475541 * t * rh
        - 6.83783e-3 * t**2
        - 5.481717e-2 * rh**2
        + 1.22874e-3 * t**2 * rh
        + 8.5282e-4 * t * rh**2
        - 1.99e-6 * t**2 * rh**2
    )
    with np.errstate(invalid="ignore"):
        dry = np.where((rh < 13) & (t >= 80) & (t <= 112), (13 - rh) / 4 * np.sqrt((17 - np.abs(t - 95)) / 17), 0.0)
    humid = np.where((rh > 85) & (t >= 80) & (t <= 87), (rh - 85) / 10 * (87 - t) / 5, 0.0)
    fahrenheit = np.where((simple + t) / 2 >= 80, regression - dry + humid, simple)
    return (fahrenheit - 32) * 5 / 9


def feels_like(hourly: pd.DataFrame) -> pd.Series:
    """
    The feels-like temperature of each hour of a weather (archive.HOURLY), to a tenth of a degree as Open-Meteo gives
    the temperature: the heat index in the heat, the wind chill in the cold wind, else the air temperature.
    """
    temperature = hourly["temperature_c"].to_numpy(dtype=float)
    humidity = hourly["humidity_pct"].to_numpy(dtype=float)
    wind = hourly["wind_speed_kmh"].to_numpy(dtype=float)
    with np.errstate(invalid="ignore"):
        feels = np.where(
            temperature >= HEAT_INDEX_C,
            heat_index(temperature, humidity),
            np.where(
                (temperature <= WIND_CHILL_C) & (wind > WIND_CHILL_KMH), wind_chill(temperature, wind), temperature
            ),
        )
    return pd.Series(np.round(feels, 1), index=hourly.index, name="feels_like_c")


def why(temperature_c: float, feels_c: float) -> str:
    """Why a feels-like temperature differs from the air's, e.g. "colder for the wind", or "" when it does not."""
    if pd.isna(temperature_c) or pd.isna(feels_c) or round(feels_c - temperature_c, 1) == 0:
        return ""
    if temperature_c >= HEAT_INDEX_C:
        return "warmer for the humidity" if feels_c > temperature_c else "cooler for the dry air"
    return "colder for the wind"
