"""
How warm or cold the weather feels: the Universal Thermal Climate Index (UTCI), the feels-like temperature of the
thermal comfort data of ECMWF and Copernicus (ERA5-HEAT), computed with thermofeel, the library of ECMWF.

UTCI is the air temperature of a reference outdoor environment (a light breeze, 50 % humidity, no sun) in which a
person dressed for the weather feels as in the actual weather: it weighs the air temperature, the humidity, the wind at
10 m and the radiation the body gets, as its mean radiant temperature (MRT). Open-Meteo has the sunlight at the hour
from the same models: on the ground, its direct beam on the ground and the beam across its path. From these the MRT
follows Di Napoli et al. (2020), as for ERA5-HEAT, with what Open-Meteo lacks estimated: the long-wave radiation of
the sky from the air temperature, the humidity and the cloud cover (Prata 1996 under a clear sky, Unsworth and
Monteith 1975 under clouds), that of the ground from the air temperature, and a ground reflecting a fifth of the
sunlight, as grass or a town does (snow, which reflects more and warms a person in the sun, is not in ERA5 on
Open-Meteo).

In the shade the body gets no direct beam: the light of the sky and of the ground only.
"""

import numpy as np
import pandas as pd
import thermofeel

STEFAN_BOLTZMANN = 5.67e-8  # W/m²/K⁴
ALBEDO = 0.2
GROUND_EMISSIVITY = 0.97
# The ranges the UTCI polynomial is fitted on (Bröde et al. 2012): beyond them it extrapolates
WIND_MS = (0.5, 17.0)
RADIANT_EXCESS_C = (-30.0, 70.0)  # MRT minus the air temperature
VAPOUR_HPA = 50.0
# The UTCI assessment scale, from the hottest: the least UTCI of each category
STRESS = [
    (46.0, "Extreme heat stress"),
    (38.0, "Very strong heat stress"),
    (32.0, "Strong heat stress"),
    (26.0, "Moderate heat stress"),
    (9.0, "No thermal stress"),
    (0.0, "Slight cold stress"),
    (-13.0, "Moderate cold stress"),
    (-27.0, "Strong cold stress"),
    (-40.0, "Very strong cold stress"),
]
COMFORT = (9.0, 26.0)  # no thermal stress between these


def vapour_pressure(temperature_c: np.ndarray, humidity_pct: np.ndarray) -> np.ndarray:
    """Water vapour pressure of the air, in hPa."""
    return humidity_pct / 100 * thermofeel.calculate_saturation_vapour_pressure(temperature_c + 273.15)


def sky_longwave(temperature_c: np.ndarray, humidity_pct: np.ndarray, cloud_cover_pct: np.ndarray) -> np.ndarray:
    """
    The long-wave radiation of the sky on the ground, in W/m²: of a clear sky from its precipitable water (Prata 1996),
    the clouds radiating as a body at the air temperature, 84 % of their cover (Unsworth and Monteith 1975).
    """
    kelvin = temperature_c + 273.15
    water_cm = 46.5 * vapour_pressure(temperature_c, humidity_pct) / kelvin
    clear = 1 - (1 + water_cm) * np.exp(-np.sqrt(1.2 + 3 * water_cm))
    clouds = 0.84 * cloud_cover_pct / 100
    return ((1 - clouds) * clear + clouds) * STEFAN_BOLTZMANN * kelvin**4


def mean_radiant_temperature(
    temperature_c: np.ndarray,
    humidity_pct: np.ndarray,
    cloud_cover_pct: np.ndarray,
    radiation_wm2: np.ndarray,
    direct_radiation_wm2: np.ndarray,
    direct_normal_wm2: np.ndarray,
    shade: bool = False,
) -> np.ndarray:
    """
    The mean radiant temperature of a person standing outdoors, in °C, from the sunlight on the ground, its direct beam
    on the ground and across its path, in W/m², and the long-wave radiation of the sky and the ground; in the shade,
    without the direct beam.
    """
    sky = sky_longwave(temperature_c, humidity_pct, cloud_cover_pct)
    ground = GROUND_EMISSIVITY * STEFAN_BOLTZMANN * (temperature_c + 273.15) ** 4 + (1 - GROUND_EMISSIVITY) * sky
    # The height of the sun from its beam on the ground and across it: without a beam it does not count
    with np.errstate(divide="ignore", invalid="ignore"):
        sun_height = np.where(direct_normal_wm2 > 0, np.clip(direct_radiation_wm2 / direct_normal_wm2, 0, 1), 0.0)
    kelvin = thermofeel.calculate_mean_radiant_temperature(
        ssrd=radiation_wm2,
        ssr=radiation_wm2 * (1 - ALBEDO),
        dsrp=np.zeros_like(direct_normal_wm2) if shade else direct_normal_wm2,
        strd=sky,
        fdir=direct_radiation_wm2,
        strr=sky - ground,
        cossza=sun_height,
    )
    return kelvin - 273.15


def utci(
    temperature_c: np.ndarray, humidity_pct: np.ndarray, wind_kmh: np.ndarray, radiant_c: np.ndarray
) -> np.ndarray:
    """The UTCI in °C, its inputs brought within the ranges of its polynomial."""
    radiant = np.clip(radiant_c - temperature_c, *RADIANT_EXCESS_C) + temperature_c
    kelvin = thermofeel.calculate_utci(
        t2_k=temperature_c + 273.15,
        va=np.clip(wind_kmh / 3.6, *WIND_MS),
        mrt=radiant + 273.15,
        ehPa=np.minimum(vapour_pressure(temperature_c, humidity_pct), VAPOUR_HPA),
    )
    return kelvin - 273.15


def feels_like(hourly: pd.DataFrame) -> pd.DataFrame:
    """
    The feels-like temperature of each hour of a weather (archive.HOURLY), in the sun as it shines then and in the
    shade: feels_like_c and feels_like_shade_c, missing where the hour misses one of its inputs.
    """
    columns = ["temperature_c", "humidity_pct", "cloud_cover_pct", "radiation_wm2", "direct_radiation_wm2",
               "direct_normal_wm2", "wind_speed_kmh"]  # fmt: skip
    values = {column: hourly[column].to_numpy(dtype=float) for column in columns}
    wind = values.pop("wind_speed_kmh")
    feels = {}
    with np.errstate(invalid="ignore"):
        for name, shade in [("feels_like_c", False), ("feels_like_shade_c", True)]:
            radiant = mean_radiant_temperature(**values, shade=shade)
            feels[name] = utci(values["temperature_c"], values["humidity_pct"], wind, radiant)
    return pd.DataFrame(feels, index=hourly.index)


def stress(value: float) -> str:
    """The UTCI category of a feels-like temperature, e.g. "Moderate heat stress", or "–" without one."""
    if pd.isna(value):
        return "–"
    return next((label for least, label in STRESS if value >= least), "Extreme cold stress")
