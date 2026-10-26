"""
Weather of a place on demand, from Open-Meteo: the ERA5 reanalysis, then ECMWF IFS for the last days and
the forecast.

Open-Meteo stores the ERA5 reanalysis as time series: a month of one place takes a single small request,
where the ARCO-ERA5 Zarr store of the pipeline would send the global field of every hour. ERA5 comes about
five days behind real time; the operational model of the same centre, ECMWF IFS (9 km), fills those days
from its recent runs and goes on with its forecast.
"""

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from dashboard.places import USER_AGENT

FIRST_DAY = date(1940, 1, 1)
# ERA5 is published about five days behind real time: the archive has complete days up to six days ago
DELAY = timedelta(days=6)
# Days of ECMWF IFS forecast, today included: the forecast API goes 15 days ahead, the last one incomplete
FORECAST_DAYS = 15


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    model: str


ERA5 = Source("ERA5", "https://archive-api.open-meteo.com/v1/archive", "era5")  # the 0.25° grid of the pipeline
IFS = Source("ECMWF IFS", "https://api.open-meteo.com/v1/forecast", "ecmwf_ifs")
FORECAST = "Forecast"  # the days and hours of IFS from now on
SOURCES = [ERA5.name, IFS.name, FORECAST]
# The wet_day_mm threshold of the dbt models
WET_DAY_MM = 1.0

HOURLY = {
    "temperature_2m": "temperature_c",
    "precipitation": "precipitation_mm",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_direction_10m": "wind_direction_deg",  # where the wind blows from, clockwise from north
    "wind_gusts_10m": "wind_gusts_kmh",
    "relative_humidity_2m": "humidity_pct",
    "cloud_cover": "cloud_cover_pct",
    "sunshine_duration": "sunshine_s",  # in the hour
    "weather_code": "weather_code",  # WMO, see WEATHER
    "is_day": "is_day",
}
# The fields of an hour that come from the model: is_day is astronomical, given for the hours not published yet
MODELLED = [column for column in HOURLY.values() if column != "is_day"]
COMPASS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

# The WMO weather codes, which Open-Meteo derives from the clouds, precipitation and snow of a model:
# their label, and their icon by day and by night
WEATHER = {
    0: ("Clear", "☀️", "🌙"),
    1: ("Mainly clear", "🌤️", "🌙"),
    2: ("Partly cloudy", "⛅", "☁️"),
    3: ("Overcast", "☁️", "☁️"),
    45: ("Fog", "🌫️", "🌫️"),
    48: ("Rime fog", "🌫️", "🌫️"),
    51: ("Light drizzle", "🌦️", "🌧️"),
    53: ("Drizzle", "🌦️", "🌧️"),
    55: ("Dense drizzle", "🌧️", "🌧️"),
    56: ("Freezing drizzle", "🌧️", "🌧️"),
    57: ("Freezing drizzle", "🌧️", "🌧️"),
    61: ("Light rain", "🌦️", "🌧️"),
    63: ("Rain", "🌧️", "🌧️"),
    65: ("Heavy rain", "🌧️", "🌧️"),
    66: ("Freezing rain", "🌧️", "🌧️"),
    67: ("Freezing rain", "🌧️", "🌧️"),
    71: ("Light snow", "🌨️", "🌨️"),
    73: ("Snow", "🌨️", "🌨️"),
    75: ("Heavy snow", "❄️", "❄️"),
    77: ("Snow grains", "🌨️", "🌨️"),
    80: ("Light showers", "🌦️", "🌧️"),
    81: ("Showers", "🌧️", "🌧️"),
    82: ("Violent showers", "⛈️", "⛈️"),
    85: ("Snow showers", "🌨️", "🌨️"),
    86: ("Heavy snow showers", "❄️", "❄️"),
    95: ("Thunderstorm", "⛈️", "⛈️"),
    96: ("Thunderstorm with hail", "⛈️", "⛈️"),
    99: ("Thunderstorm with hail", "⛈️", "⛈️"),
}

# Statistics of a period of daily weather, as named aggregations: the averages of the highs, lows and means,
# the extremes and the totals
STATISTICS = {
    "temperature_max_mean_c": ("temperature_max_c", "mean"),
    "temperature_min_mean_c": ("temperature_min_c", "mean"),
    "temperature_mean_c": ("temperature_mean_c", "mean"),
    "temperature_max_c": ("temperature_max_c", "max"),
    "temperature_min_c": ("temperature_min_c", "min"),
    "precipitation_mm": ("precipitation_mm", "sum"),
    "wet_days": ("precipitation_mm", lambda mm: int((mm >= WET_DAY_MM).sum())),
    "wind_speed_mean_kmh": ("wind_speed_mean_kmh", "mean"),
    "wind_gusts_max_kmh": ("wind_gusts_max_kmh", "max"),
    "sunshine_h": ("sunshine_h", "sum"),
    "humidity_mean_pct": ("humidity_mean_pct", "mean"),
    "days": ("date", "count"),
}


@dataclass(frozen=True)
class Weather:
    """Daily and hourly weather of the grid point of a model nearest to a place, in the local time of the place."""

    latitude: float  # of the grid point of the first source
    longitude: float
    elevation: float  # m
    timezone: str  # e.g. Europe/Rome
    # date, temperature_max_c, temperature_mean_c, temperature_min_c, precipitation_mm, wind_speed_mean_kmh,
    # wind_speed_max_kmh, wind_gusts_max_kmh, wind_direction_deg (the dominant one), humidity_mean_pct,
    # humidity_min_pct, humidity_max_pct, cloud_cover_mean_pct, sunshine_h, weather_code (the most severe of
    # the daylight hours), source (one of SOURCES)
    daily: pd.DataFrame
    # time (in the local time zone), temperature_c, precipitation_mm, wind_speed_kmh, wind_direction_deg,
    # wind_gusts_kmh, humidity_pct, cloud_cover_pct, sunshine_s, weather_code, is_day, source
    hourly: pd.DataFrame
    # The grid point of each model: (latitude, longitude, elevation in m), by source name
    points: dict[str, tuple[float, float, float]] = field(default_factory=dict)


def compass(degrees: float) -> str:
    """The 16-point compass name of a direction, e.g. NNE."""
    return "–" if pd.isna(degrees) else COMPASS[round(degrees / 22.5) % 16]


def sky(code: float, by_day: bool = True) -> tuple[str, str]:
    """The icon and the label of a WMO weather code, e.g. ("⛅", "Partly cloudy"), or ("", "–") without one."""
    if pd.isna(code) or int(code) not in WEATHER:
        return "", "–"
    label, day_icon, night_icon = WEATHER[int(code)]
    return day_icon if by_day else night_icon, label


def last_day(today: date | None = None) -> date:
    """
    The most recent day of the archive, up to 23:00 UTC: west of Greenwich its local evening is not
    published yet (in New York, the hours after 19:00).
    """
    return (today or date.today()) - DELAY


def last_complete_day(today: date | None = None) -> date:
    """The most recent day of ERA5 published completely in every time zone."""
    return last_day(today) - timedelta(days=1)


def last_forecast_day(today: date | None = None) -> date:
    """The last day of the ECMWF IFS forecast complete in every time zone."""
    return (today or date.today()) + timedelta(days=FORECAST_DAYS - 1)


def monthly(daily: pd.DataFrame) -> pd.DataFrame:
    """The STATISTICS of each month of a daily history, one row per month (its first day)."""
    month = daily["date"].dt.to_period("M").dt.to_timestamp().rename("month")
    return daily.groupby(month).agg(**STATISTICS).reset_index()


def summary(daily: pd.DataFrame) -> pd.Series:
    """The STATISTICS of all the days of a daily history, which must have some."""
    # As objects, so that the counts stay integers in a row of mixed types
    return daily.groupby(pd.Series(0, index=daily.index)).agg(**STATISTICS).astype(object).iloc[0]


WEATHER_PARTS = ["daylight_clouds", "daylight_storm_code", "precipitation_code", "daylight_precipitation_code",
                 "fog_hours"]  # fmt: skip


def weather_of_days(daily: pd.DataFrame) -> pd.Series:
    """
    The WMO code of each day from the WEATHER_PARTS of its hours, as a forecast sums a day up: a thunderstorm
    by daylight; on a wet day its most severe precipitation, by daylight if any; fog for 3 daylight hours or
    more; else the mean cloud cover of the daylight hours, in the steps of codes 0 to 3. The most severe code
    of the hours would call a day of 10 hours of sun overcast, for a single overcast hour.
    """
    clouds = daily["daylight_clouds"].fillna(daily["cloud_cover_mean_pct"])  # all the hours in a polar night
    cloudiness = np.select([clouds < 20, clouds < 40, clouds < 70], [0, 1, 2], 3)
    storm = daily["daylight_storm_code"]
    wet = daily["precipitation_mm"] >= WET_DAY_MM
    # A wet day without a precipitation code in its hours is light rain
    precipitation = daily["daylight_precipitation_code"].fillna(daily["precipitation_code"]).fillna(61)
    codes = np.select([storm.notna(), wet, daily["fog_hours"] >= 3], [storm, precipitation, 45], cloudiness)
    return pd.Series(codes, index=daily.index, dtype=float).where(clouds.notna() | wet | storm.notna())


class RequestLimit(RuntimeError):
    """
    Open-Meteo refused a request: its free calls of a minute, an hour or a day are used up. It counts a request
    of more than two weeks as several calls, a year of a place as 26.
    """


def refusal(error: urllib.error.HTTPError) -> str:
    """The reason Open-Meteo gives for refusing a request."""
    try:
        return str(json.load(error)["reason"])
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return "too many requests, try again later."


def get_json(url: str, attempts: int = 3) -> Any:
    """
    The JSON answer of a URL, asked again after a pause when the server fails (5xx) or the network does. A
    refusal for the request limit comes at once, with its reason: asking again would not help.
    """
    for attempt in range(attempts):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=20) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 429:
                raise RequestLimit(f"Open-Meteo refused the request: {refusal(error)}") from None
            if error.code < 500 or attempt == attempts - 1:
                raise
        except urllib.error.URLError:
            if attempt == attempts - 1:
                raise
        time.sleep(0.5 * 2**attempt)
    raise AssertionError("unreachable")


def fetch(latitude: float, longitude: float, start: date, end: date, source: Source = ERA5) -> Weather:
    """
    Weather of the local days of a place from start to end (inclusive), from a source; the days it does not
    have completely are left out.

    Open-Meteo finds the time zone of the place, but shifts all the hours of an answer by the UTC offset
    of today: an hour off on the other side of daylight saving time. So the hours are shifted back to UTC,
    converted to the time zone with its rules of every year, and the local days (23 to 25 hours long)
    are aggregated here.
    """
    params = urllib.parse.urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            # A local day spans two UTC days
            "start_date": max(start - timedelta(days=1), FIRST_DAY).isoformat(),
            "end_date": (end + timedelta(days=1)).isoformat(),
            "hourly": ",".join(HOURLY),
            "models": source.model,
            "timezone": "auto",
        }
    )
    answer = get_json(f"{source.url}?{params}")
    hourly = pd.DataFrame(answer["hourly"]).rename(columns=HOURLY).dropna(subset=MODELLED, how="all")
    utc = pd.to_datetime(hourly["time"]) - pd.Timedelta(seconds=answer["utc_offset_seconds"])
    hourly["time"] = utc.dt.tz_localize("UTC").dt.tz_convert(answer["timezone"])
    hourly = hourly[hourly["time"].dt.date.between(start, end)].reset_index(drop=True)

    # The dominant direction of a day is that of the sum of its hourly winds as vectors: the plain mean of
    # 350° and 10° would be 180°, the south
    blowing = np.radians(hourly["wind_direction_deg"])
    daylight = hourly["is_day"] == 1
    code = hourly["weather_code"]
    vectors = hourly.assign(
        east=hourly["wind_speed_kmh"] * np.sin(blowing),
        north=hourly["wind_speed_kmh"] * np.cos(blowing),
        # What the weather of a day is made of: see weather_of_days
        daylight_clouds=hourly["cloud_cover_pct"].where(daylight),
        daylight_storm_code=code.where(daylight & (code >= 95)),
        precipitation_code=code.where(code >= 51),
        daylight_precipitation_code=code.where(daylight & (code >= 51)),
        fog_hours=daylight & code.isin([45, 48]),
    )
    daily = vectors.groupby(hourly["time"].dt.date.rename("date")).agg(
        temperature_max_c=("temperature_c", "max"),
        temperature_mean_c=("temperature_c", "mean"),
        temperature_min_c=("temperature_c", "min"),
        precipitation_mm=("precipitation_mm", "sum"),
        wind_speed_mean_kmh=("wind_speed_kmh", "mean"),
        wind_speed_max_kmh=("wind_speed_kmh", "max"),
        wind_gusts_max_kmh=("wind_gusts_kmh", "max"),
        humidity_mean_pct=("humidity_pct", "mean"),
        humidity_min_pct=("humidity_pct", "min"),
        humidity_max_pct=("humidity_pct", "max"),
        cloud_cover_mean_pct=("cloud_cover_pct", "mean"),
        sunshine_s=("sunshine_s", "sum"),
        daylight_clouds=("daylight_clouds", "mean"),
        daylight_storm_code=("daylight_storm_code", "max"),
        precipitation_code=("precipitation_code", "max"),
        daylight_precipitation_code=("daylight_precipitation_code", "max"),
        fog_hours=("fog_hours", "sum"),
        east=("east", "sum"),
        north=("north", "sum"),
        hours=("time", "count"),
    )
    daily["wind_direction_deg"] = np.degrees(np.arctan2(daily.pop("east"), daily.pop("north"))) % 360
    daily["sunshine_h"] = daily.pop("sunshine_s") / 3600
    daily["weather_code"] = weather_of_days(daily)
    daily = daily.drop(columns=WEATHER_PARTS)
    # Complete days only, 23 hours long on the day clocks go forward
    daily = daily[daily["hours"] >= 23].drop(columns="hours").reset_index()
    daily["date"] = pd.to_datetime(daily["date"])
    point = (answer["latitude"], answer["longitude"], answer["elevation"])
    return Weather(
        *point,
        timezone=answer["timezone"],
        daily=daily.assign(source=source.name),
        hourly=hourly.assign(source=source.name),
        points={source.name: point},
    )


Period = tuple[date, date]


def periods(start: date, end: date, now: pd.Timestamp) -> tuple[Period | None, Period | None]:
    """
    The days from start to end to ask ERA5, as far as it goes, and ECMWF IFS, from the day before the last
    of ERA5, whose local day may lack its last hours west of Greenwich; None for a source with no day.
    """
    era5_end = min(end, last_day(now.date()))
    ifs_start = max(start, last_day(now.date()) - timedelta(days=1))
    return (start, era5_end) if start <= era5_end else None, (ifs_start, end) if ifs_start <= end else None


def fetch_with_forecast(
    latitude: float, longitude: float, start: date, end: date, now: pd.Timestamp | None = None
) -> Weather:
    """
    Weather of a place from start to end, from ERA5 as far as it goes, then from ECMWF IFS: its recent runs
    for the days ERA5 has not published yet, and its forecast from now on.
    """
    now = pd.Timestamp.now(tz="UTC") if now is None else now
    era5, ifs = periods(start, end, now)
    reanalysis = fetch(latitude, longitude, *era5, ERA5) if era5 else None
    operational = fetch(latitude, longitude, *ifs, IFS) if ifs else None
    return combine(reanalysis, operational, now)


def combine(reanalysis: Weather | None, operational: Weather | None, now: pd.Timestamp) -> Weather:
    """
    The weather of ERA5 followed by that of ECMWF IFS, either of which may be missing: the days and hours
    of IFS from now on are the forecast. A day or an hour both have comes from ERA5.
    """
    if operational is None:
        assert reanalysis is not None, "no weather to combine"
        return reanalysis

    daily, hourly = operational.daily, operational.hourly
    local_now = now.tz_convert(operational.timezone)
    daily = daily.assign(source=np.where(daily["date"].dt.date >= local_now.date(), FORECAST, IFS.name))
    hourly = hourly.assign(source=np.where(hourly["time"] >= local_now, FORECAST, IFS.name))
    if reanalysis is None:
        return replace(operational, daily=daily, hourly=hourly)
    if not reanalysis.hourly.empty:
        hourly = hourly[hourly["time"] > reanalysis.hourly["time"].max()]
    return Weather(
        reanalysis.latitude,
        reanalysis.longitude,
        reanalysis.elevation,
        reanalysis.timezone,
        daily=pd.concat([reanalysis.daily, daily[~daily["date"].isin(reanalysis.daily["date"])]], ignore_index=True),
        hourly=pd.concat([reanalysis.hourly, hourly], ignore_index=True),
        points={**reanalysis.points, **operational.points},
    )
