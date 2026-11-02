"""
The weather of a place at a glance, as a weather app shows it, a Streamlit component (forecast.js): the day of the page
first, now when it is today, with its sky, its temperatures and how warm or cold they feel (comfort.py), and its night,
morning, afternoon and evening; then its hours, the next HOURS from now when it is today; the next DAYS days, today
first, a day clicked becoming the day of the page; and the details of the day in cards: its temperatures against the
averages of its month, how they feel, its precipitation, wind, sunshine and humidity, and the altitude of the place.
"""

from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from dashboard import archive, comfort, icons, terrain

HOURS = 24
# Ten days, today included: the skill of a forecast fades after them, and the row fits a computer's screen
DAYS = 10
# The parts of a day, six hours from their first, and the precipitation that makes one wet, as 1 mm makes a day
PARTS = [("Night", 0), ("Morning", 6), ("Afternoon", 12), ("Evening", 18)]
WET_PART_MM = 0.5
# The least precipitation of an hour that a rain gauge measures
WET_HOUR_MM = 0.1
# The feels-like temperature in the shade is worth showing this much below the one in the sun
SHADE_C = 0.5
# The Beaufort scale of the WMO: the least speed of each force from 1, in m/s, and its name
BEAUFORT = [(0.3, "Light air"), (1.6, "Light breeze"), (3.4, "Gentle breeze"), (5.5, "Moderate breeze"),
            (8.0, "Fresh breeze"), (10.8, "Strong breeze"), (13.9, "Near gale"), (17.2, "Gale"), (20.8, "Strong gale"),
            (24.5, "Storm"), (28.5, "Violent storm"), (32.7, "Hurricane force")]  # fmt: skip
WIND_HELP = "Wind at 10 m above the ground, the mean of the hourly speeds"
SUNSHINE_HELP = "Hours with direct sunlight, of more than 120 W/m² as the WMO defines sunshine"
HUMIDITY_HELP = "Relative humidity at 2 m above the ground, the mean of the hourly values"
FEELS_LIKE_HELP = (
    "UTCI, the Universal Thermal Climate Index of ECMWF and Copernicus: the temperature that would feel the same "
    "with a light breeze, 50 % humidity and no sun, from the temperature, humidity, wind and sunlight of each hour, "
    "in the sun when it shines"
)
ALTITUDE_HELP = (
    "Mean of the altitudes of 49 points spread over the hexagon of the place, from the Copernicus 90 m elevation "
    "model, with their lowest and highest; the sea counts as 0 m"
)
# The rows scroll sideways on a phone. On the light theme the white clouds of the icons get a shadow, to stand out.
CSS = """
.forecast {color: inherit; font-variant-numeric: tabular-nums}
.forecast .heading {font-size: 0.85rem; font-weight: 600; opacity: 0.75; margin: 16px 0 6px}
.forecast .row {display: flex; gap: 6px; overflow-x: auto; padding-bottom: 8px; scrollbar-width: thin}
.forecast .number {font-family: "Chakra Petch", sans-serif; font-weight: 600}
.hero {
  display: flex; align-items: center; flex-wrap: wrap; gap: 4px 18px; padding: 12px 20px 14px 8px; margin-bottom: 8px;
  border-radius: 14px; background: color-mix(in srgb, var(--accent) 8%, transparent);
}
.hero .icon {width: 112px; height: 112px}
.hero .temperature {font-size: 3.6rem; line-height: 1; letter-spacing: -0.02em}
.hero .temperature .low {font-size: 1.8rem; opacity: 0.6; margin-left: 4px}
.hero .when {font-size: 0.85rem; font-weight: 600; opacity: 0.75}
.hero .sky {font: 600 1.45rem "Chakra Petch", sans-serif; line-height: 1.3}
.hero .feels {font-weight: 600; margin-top: 2px}
.hero .line {opacity: 0.85}
.parts {display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 6px; max-width: 560px}
.part, .card {
  border-radius: 10px; border: 1px solid color-mix(in srgb, currentColor 15%, transparent);
  background: color-mix(in srgb, currentColor 3%, transparent);
}
.part {display: flex; flex-direction: column; align-items: center; gap: 1px; padding: 6px 2px}
.forecast .hour, .forecast .day {
  flex: 0 0 auto; display: flex; flex-direction: column; align-items: center; gap: 2px; border-radius: 10px;
}
.forecast .hour {width: 62px; padding: 4px 0}
.forecast .day {
  width: 80px; padding: 8px 2px; font: inherit; color: inherit; cursor: pointer;
  border: 1px solid color-mix(in srgb, currentColor 15%, transparent);
  background: color-mix(in srgb, currentColor 3%, transparent);
}
.forecast .day:hover {border-color: var(--accent)}
.forecast .day.selected {
  border: 2px solid var(--accent); padding: 7px 1px; background: color-mix(in srgb, var(--accent) 10%, transparent);
}
.forecast .over {font-size: 0.75rem; font-weight: 600; min-height: 1.1em}
.forecast .label, .forecast .muted {font-size: 0.8rem; opacity: 0.75}
.forecast .name {font-weight: 600; font-size: 0.9rem}
.forecast .high {font-weight: 600}
.forecast .feels-like {font-size: 0.72rem; opacity: 0.75; text-align: center; line-height: 1.2}
.forecast .row .icon {width: 44px; height: 44px}
.part .icon {width: 40px; height: 40px}
.forecast .icon {filter: drop-shadow(0 1px 1.5px rgba(28, 36, 48, 0.35))}
.forecast.dark .icon {filter: none}
.forecast .rain {display: flex; align-items: center; font-size: 0.75rem; opacity: 0.85; min-height: 1.2em}
.forecast .drop {width: 16px; height: 16px}
.forecast .track {
  position: relative; width: 6px; height: 36px; margin: 2px 0; border-radius: 3px;
  background: color-mix(in srgb, currentColor 10%, transparent);
}
.forecast .bar {position: absolute; left: 0; right: 0; border-radius: 3px; background: var(--accent)}
.cards {display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 8px}
.card {padding: 10px 12px}
.card .head {
  display: flex; align-items: center; gap: 2px; font-size: 0.72rem; font-weight: 600; letter-spacing: 0.05em;
  text-transform: uppercase; opacity: 0.75;
}
.card .head .icon {width: 30px; height: 30px; margin: -6px 0 -6px -6px}
.card .head .emoji {font-size: 1.05rem; width: 24px; text-align: center}
.card .value {font-size: 1.45rem; margin: 6px 0 2px; display: flex; align-items: center; gap: 8px}
.card .arrow {display: inline-block; font-size: 1.2rem; color: var(--accent)}
.card .line {font-size: 0.85rem; opacity: 0.85}
"""

FORECAST = st.components.v2.component(
    "forecast", js=Path(__file__).with_name("forecast.js").read_text(encoding="utf-8"), css=CSS
)


def degrees(value: float | None) -> str:
    return "–" if value is None or pd.isna(value) else f"{value:.1f}°"


def precipitation(mm: float) -> str:
    """The precipitation of an hour or a day, empty when there is none to its tenth of a millimeter."""
    return "" if pd.isna(mm) or round(mm, 1) == 0 else f"{mm:.1f} mm"


def beaufort(kmh: float) -> tuple[int, str]:
    """The force of a wind on the Beaufort scale, and its name."""
    force = sum(kmh / 3.6 >= least for least, _ in BEAUFORT)
    return force, "Calm" if force == 0 else BEAUFORT[force - 1][1]


def stress_of_day(low: float, high: float) -> str:
    """The thermal stress of a day from its lowest and highest feels-like: the one further from comfort."""
    hot, cold = high - comfort.COMFORT[1], comfort.COMFORT[0] - low
    if hot < 0 and cold <= 0:
        return comfort.stress(high)
    return f"{comfort.stress(high)} at the warmest" if hot >= cold else f"{comfort.stress(low)} at the coldest"


def hour_items(hourly: pd.DataFrame, from_now: bool) -> list[dict[str, Any]]:
    """The hours of a row, the first one "Now" when they start from the current hour."""
    items = []
    for index, hour in enumerate(hourly.to_dict("records")):
        by_day = hour["is_day"] == 1
        emoji, label = archive.sky(hour["weather_code"], by_day)
        items.append(
            {
                # The day above its first hour, and above midnight
                "over": f"{hour['time']:%a}" if index == 0 or hour["time"].hour == 0 else "",
                "label": "Now" if from_now and index == 0 else f"{hour['time']:%H:%M}",
                "icon": icons.url(hour["weather_code"], by_day),
                "sky": f"{emoji} {label}",
                "temperature": degrees(hour["temperature_c"]),
                "feels": degrees(hour.get("feels_like_c")),
                "rain": precipitation(hour["precipitation_mm"]),
            }
        )
    return items


def day_items(daily: pd.DataFrame, today: date, selected: date) -> list[dict[str, Any]]:
    """
    The days of a row from today; the selected one is marked. The bar of a day goes from its low to its high, as
    fractions from the top of the scale of the days: the highest high at 0, the lowest low at 1.
    """
    top, bottom = daily["temperature_max_c"].max(), daily["temperature_min_c"].min()
    span = max(float(top - bottom), 1.0) if pd.notna(top) and pd.notna(bottom) else 1.0
    items = []
    for row in daily.to_dict("records"):
        when = row["date"].date()
        emoji, label = archive.sky(row["weather_code"])
        name = {today: "Today", today + timedelta(days=1): "Tomorrow"}.get(when, f"{when:%a}")
        high, low = row["temperature_max_c"], row["temperature_min_c"]
        known = pd.notna(high) and pd.notna(low)
        items.append(
            {
                "date": when.isoformat(),
                "name": name,
                "date_label": f"{when.day} {when:%b}",
                "title": f"{when:%A %d %B}: {label}",
                "icon": icons.url(row["weather_code"]),
                "sky": f"{emoji} {label}",
                "high": degrees(high),
                "low": degrees(low),
                "rain": precipitation(row["precipitation_mm"]),
                "top": float((top - high) / span) if known else 0.0,
                "height": float((high - low) / span) if known else 0.0,
                "selected": when == selected,
            }
        )
    return items


def part_items(hours: pd.DataFrame) -> list[dict[str, Any]]:
    """The night, morning, afternoon and evening of the hours of a day, each with its weather, as a day has its own."""
    items = []
    for name, first in PARTS:
        part = hours[hours["time"].dt.hour.between(first, first + 5)]
        if part.empty:
            continue
        code = archive.weather_of_hours(part, WET_PART_MM)
        by_day = bool((part["is_day"] == 1).mean() >= 0.5)
        emoji, label = archive.sky(code, by_day)
        items.append(
            {
                "name": name,
                "hours": f"{first:02d}–{first + 6:02d}",
                "icon": icons.url(code, by_day),
                "sky": f"{emoji} {label}",
                "high": degrees(part["temperature_c"].max()),
                "low": degrees(part["temperature_c"].min()),
                "rain": precipitation(part["precipitation_mm"].sum()),
            }
        )
    return items


def hero(day: date, day_weather: pd.Series | None, now: pd.Series | None) -> dict[str, Any] | None:
    """
    The head of the day: its weather now when it is today (now, its current hour), else its sky and temperatures,
    with how they feel, in the sun and in the shade; None without either.
    """
    if now is not None:
        by_day = now["is_day"] == 1
        emoji, label = archive.sky(now["weather_code"], by_day)
        sun, shade = now.get("feels_like_c"), now.get("feels_like_shade_c")
        shaded = pd.notna(sun) and pd.notna(shade) and sun - shade >= SHADE_C
        lines = [f"In the shade {degrees(shade)}"] if shaded else []
        if day_weather is not None:
            lines.append(
                f"High {degrees(day_weather['temperature_max_c'])} · Low {degrees(day_weather['temperature_min_c'])}"
            )
        return {
            "icon": icons.url(now["weather_code"], by_day, animated=True),
            "sky": label,
            "alt": f"{emoji} {label}",
            "when": f"Now · {day:%A %d %B %Y}",
            "temperature": degrees(now["temperature_c"]),
            "low": "",
            "feels": "" if pd.isna(sun) else f"Feels like {degrees(sun)} · {comfort.stress(sun)}",
            "lines": lines,
        }
    if day_weather is None:
        return None
    emoji, label = archive.sky(day_weather["weather_code"])
    low, high = day_weather.get("feels_like_min_c", np.nan), day_weather.get("feels_like_max_c", np.nan)
    shade = day_weather.get("feels_like_shade_max_c", np.nan)
    known = pd.notna(low) and pd.notna(high)
    shaded = known and pd.notna(shade) and high - shade >= SHADE_C
    return {
        "icon": icons.url(day_weather["weather_code"], animated=True),
        "sky": label,
        "alt": f"{emoji} {label}",
        "when": f"{day:%A %d %B %Y}",
        "temperature": degrees(day_weather["temperature_max_c"]),
        "low": f"/ {degrees(day_weather['temperature_min_c'])}",
        "feels": f"Feels like {degrees(low)} to {degrees(high)} · {stress_of_day(low, high)}" if known else "",
        "lines": [f"Up to {degrees(shade)} in the shade"] if shaded else [],
    }


def card(icon: str | None, label: str, value: str, lines: list[str], title: str = "", **extra: Any) -> dict[str, Any]:
    """A card of the details: a Meteocons icon (or an emoji, in extra), a label, a value and lines under it."""
    return {"icon": icon, "label": label, "value": value, "lines": [line for line in lines if line], "title": title,
            **extra}  # fmt: skip


def details(
    day_weather: pd.Series | None,
    hours: pd.DataFrame,
    month: pd.Series | None,
    altitude: terrain.Altitude | None,
) -> list[dict[str, Any]]:
    """
    The cards of the details of a day: its temperatures, against the averages of the days of its month gone by, how
    they feel, its precipitation and the hours it falls, its wind, sunshine and humidity; and the altitude of the place.
    """
    cards = []
    if day_weather is not None:
        high, low = day_weather["temperature_max_c"], day_weather["temperature_min_c"]
        mean = day_weather["temperature_mean_c"]
        versus = (
            []
            if month is None
            else [
                f"{name} {value - month[statistic]:+.1f}°"
                for name, value, statistic in [
                    ("high", high, "temperature_max_mean_c"),
                    ("low", low, "temperature_min_mean_c"),
                    ("average", mean, "temperature_mean_c"),
                ]
                if pd.notna(value) and pd.notna(month[statistic])
            ]
        )
        cards.append(
            card(
                f"{icons.STATIC}thermometer.svg",
                "Temperature",
                f"{degrees(high)} / {degrees(low)}",
                [f"Average {degrees(mean)}", f"vs month: {', '.join(versus)}" if versus else ""],
                "The high and the low of the day, its average, and their differences to the averages of the days of "
                "its month gone by",
            )
        )
        feels_low, feels_high = day_weather.get("feels_like_min_c", np.nan), day_weather.get("feels_like_max_c", np.nan)
        if pd.notna(feels_low) and pd.notna(feels_high):
            warmer = pd.notna(high) and feels_high >= high
            shade = day_weather.get("feels_like_shade_max_c", np.nan)
            shaded = pd.notna(shade) and feels_high - shade >= SHADE_C
            cards.append(
                card(
                    f"{icons.STATIC}thermometer-{'warmer' if warmer else 'colder'}.svg",
                    "Feels like",
                    f"{degrees(feels_high)} / {degrees(feels_low)}",
                    [
                        stress_of_day(feels_low, feels_high),
                        f"Up to {degrees(shade)} in the shade" if shaded else "",
                    ],
                    FEELS_LIKE_HELP,
                )
            )
        total = day_weather["precipitation_mm"]
        wet_hours = int((hours["precipitation_mm"] >= WET_HOUR_MM).sum())
        cards.append(
            card(
                f"{icons.STATIC}raindrops.svg",
                "Precipitation",
                "–" if pd.isna(total) else f"{total:.1f} mm",
                [f"In {wet_hours} of {len(hours)} hours" if wet_hours else "Dry all day"],
                f"The total of the day, and the hours with {WET_HOUR_MM:g} mm or more",
            )
        )
        speed, gust = day_weather["wind_speed_mean_kmh"], day_weather["wind_gusts_max_kmh"]
        force, force_name = (0, "") if pd.isna(speed) else beaufort(speed)
        direction = day_weather["wind_direction_deg"]
        cards.append(
            card(
                f"{icons.STATIC}wind-beaufort-{force}.svg",
                "Wind",
                "–" if pd.isna(speed) else f"{speed:.1f} km/h",
                [
                    f"From {archive.compass(direction)}" + ("" if pd.isna(gust) else f" · gusts {gust:.0f} km/h"),
                    f"Force {force}, {force_name.lower()}" if force_name else "",
                ],
                f"{WIND_HELP}, with the direction it mostly blows from, the strongest gust and its force on the "
                "Beaufort scale",
                # The arrow points where the wind blows: a ↓ for a wind from the north, turned by its direction
                arrow=None if pd.isna(direction) else float(direction),
            )
        )
        clouds = day_weather["cloud_cover_mean_pct"]
        cards.append(
            card(
                f"{icons.STATIC}clear-day.svg",
                "Sunshine",
                "–" if pd.isna(day_weather["sunshine_h"]) else f"{day_weather['sunshine_h']:.1f} h",
                ["" if pd.isna(clouds) else f"Clouds {clouds:.0f} %"],
                f"{SUNSHINE_HELP}, and the mean cloud cover of the day",
            )
        )
        humidity = day_weather["humidity_mean_pct"]
        cards.append(
            card(
                f"{icons.STATIC}humidity.svg",
                "Humidity",
                "–" if pd.isna(humidity) else f"{humidity:.1f} %",
                [
                    ""
                    if pd.isna(day_weather["humidity_min_pct"])
                    else f"{day_weather['humidity_min_pct']:.0f}–{day_weather['humidity_max_pct']:.0f} %"
                ],
                f"{HUMIDITY_HELP}, with the lowest and highest",
            )
        )
    cards.append(
        card(
            None,
            "Altitude",
            "–" if altitude is None else f"{altitude.mean:,.0f} m",
            [] if altitude is None else [f"{altitude.min:,.0f}–{altitude.max:,.0f} m in the hexagon"],
            ALTITUDE_HELP,
            emoji="⛰️",
        )
    )
    return cards


def data(
    upcoming: archive.Weather | None,
    weather: archive.Weather | None,
    day: date,
    now: pd.Timestamp,
    accent: str,
    dark: bool,
    phone: bool = False,
    month: pd.Series | None = None,
    altitude: terrain.Altitude | None = None,
) -> dict[str, Any] | None:
    """
    What the component shows of the weather of a place: of the day of the page, from the weather of its year, its head,
    parts and details, and its hours, or the next HOURS from now when it is today; the next DAYS days, from upcoming,
    today first. In the local time of the place; None without any weather.
    """
    known = upcoming or weather
    if known is None:
        return None
    local_now = now.tz_convert(known.timezone)
    today = local_now.date()
    source = weather or upcoming
    assert source is not None
    day_weather = source.daily[source.daily["date"].dt.date == day]
    hours = source.hourly[source.hourly["time"].dt.date == day]
    # Today, its hours from now on
    next_hours = None
    if day == today and upcoming is not None:
        next_hours = upcoming.hourly[upcoming.hourly["time"] >= local_now.floor("h")].head(HOURS)
    current = None if next_hours is None or next_hours.empty else next_hours.iloc[0]
    day_row = None if day_weather.empty else day_weather.iloc[0]
    days = (
        []
        if upcoming is None
        else day_items(upcoming.daily[upcoming.daily["date"].dt.date >= today].head(DAYS), today, day)
    )
    if current is None or next_hours is None:
        row_hours = hour_items(hours, from_now=False)
    else:
        row_hours = hour_items(next_hours, from_now=True)
    return {
        "dark": dark,
        "accent": accent,
        "drop": f"{icons.STATIC}raindrop.svg",
        "hero": hero(day, day_row, current),
        "parts": part_items(hours),
        "hours_title": f"Next {len(row_hours)} hours" if current is not None else f"{day:%A %d %B}, hour by hour",
        "hours": row_hours,
        "days_title": f"Next {len(days)} days · {'tap' if phone else 'click'} a day for its weather" if days else "",
        "days": days,
        "details_title": f"{day:%A %d %B} in detail",
        "details": details(day_row, hours, month, altitude),
    }


def show(forecast_data: Mapping[str, Any], key: str, on_day: Any) -> Any:
    """The forecast, keyed to keep it over the runs; on_day runs before the run a click on a day starts."""
    return FORECAST(key=key, data=forecast_data, on_day_change=on_day)


def clicked(value: str | None) -> date | None:
    """The day of a click on the forecast, from its trigger value, or None."""
    return date.fromisoformat(value) if value else None
