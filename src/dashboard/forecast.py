"""
The forecast of a place at a glance, a Streamlit component (forecast.js): its next HOURS hours and its next DAYS days,
today first, as a weather app shows them, each with its icon (icons.py), its temperatures and its precipitation. A
day, clicked, becomes the day of the page.
"""

from collections.abc import Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from dashboard import archive, icons

HOURS = 24
# Ten days, today included: the skill of a forecast fades after them, and the row fits a computer's screen
DAYS = 10
# The rows scroll sideways on a phone. On the light theme the white clouds of the icons get a shadow, to stand out.
CSS = """
.forecast {color: inherit; font-variant-numeric: tabular-nums}
.forecast .heading {font-size: 0.85rem; font-weight: 600; opacity: 0.75; margin: 0 0 6px}
.forecast .row {display: flex; gap: 6px; overflow-x: auto; padding-bottom: 8px; scrollbar-width: thin}
.forecast .row + .heading {margin-top: 12px}
.forecast .hour, .forecast .day {
  flex: 0 0 auto; display: flex; flex-direction: column; align-items: center; gap: 2px; border-radius: 10px;
}
.forecast .hour {width: 60px; padding: 4px 0}
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
.forecast .icon {width: 44px; height: 44px; filter: drop-shadow(0 1px 1.5px rgba(28, 36, 48, 0.35))}
.forecast.dark .icon {filter: none}
.forecast .rain {display: flex; align-items: center; font-size: 0.75rem; opacity: 0.85; min-height: 1.2em}
.forecast .drop {width: 16px; height: 16px}
.forecast .track {
  position: relative; width: 6px; height: 36px; margin: 2px 0; border-radius: 3px;
  background: color-mix(in srgb, currentColor 10%, transparent);
}
.forecast .bar {position: absolute; left: 0; right: 0; border-radius: 3px; background: var(--accent)}
"""

FORECAST = st.components.v2.component(
    "forecast", js=Path(__file__).with_name("forecast.js").read_text(encoding="utf-8"), css=CSS
)


def degrees(value: float) -> str:
    return "–" if pd.isna(value) else f"{value:.1f}°"


def precipitation(mm: float) -> str:
    """The precipitation of an hour or a day, empty when there is none to its tenth of a millimeter."""
    return "" if pd.isna(mm) or round(mm, 1) == 0 else f"{mm:.1f} mm"


def data(
    weather: archive.Weather, now: pd.Timestamp, selected: date, accent: str, dark: bool, phone: bool = False
) -> dict[str, Any]:
    """
    What the component shows of a weather: the hours from the current one and the days from today, in the local
    time of the place; the selected day of the page is marked, to be tapped on a phone and clicked elsewhere. The
    bar of a day goes from its low to its high, as fractions from the top of the scale of the days: the highest high
    at 0, the lowest low at 1.
    """
    local_now = now.tz_convert(weather.timezone)
    today = local_now.date()
    hourly = weather.hourly[weather.hourly["time"] >= local_now.floor("h")].head(HOURS)
    daily = weather.daily[weather.daily["date"].dt.date >= today].head(DAYS)
    top, bottom = daily["temperature_max_c"].max(), daily["temperature_min_c"].min()
    span = max(float(top - bottom), 1.0) if pd.notna(top) and pd.notna(bottom) else 1.0

    hours = []
    for index, hour in enumerate(hourly.to_dict("records")):
        by_day = hour["is_day"] == 1
        emoji, label = archive.sky(hour["weather_code"], by_day)
        hours.append(
            {
                # The day above its first hour, and above midnight
                "over": f"{hour['time']:%a}" if index == 0 or hour["time"].hour == 0 else "",
                "label": "Now" if index == 0 else f"{hour['time']:%H:%M}",
                "icon": icons.url(hour["weather_code"], by_day),
                "sky": f"{emoji} {label}",
                "temperature": degrees(hour["temperature_c"]),
                "rain": precipitation(hour["precipitation_mm"]),
            }
        )

    days = []
    for row in daily.to_dict("records"):
        when = row["date"].date()
        emoji, label = archive.sky(row["weather_code"])
        name = {today: "Today", today + timedelta(days=1): "Tomorrow"}.get(when, f"{when:%a}")
        high, low = row["temperature_max_c"], row["temperature_min_c"]
        known = pd.notna(high) and pd.notna(low)
        days.append(
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
    return {
        "dark": dark,
        "accent": accent,
        "drop": f"{icons.STATIC}raindrop.svg",
        "hours_title": f"Next {len(hours)} hours",
        "days_title": f"Next {len(days)} days · {'tap' if phone else 'click'} a day for its weather",
        "hours": hours,
        "days": days,
    }


def show(forecast_data: Mapping[str, Any], key: str, on_day: Any) -> Any:
    """The forecast, keyed to keep it over the runs; on_day runs before the run a click on a day starts."""
    return FORECAST(key=key, data=forecast_data, on_day_change=on_day)


def clicked(value: str | None) -> date | None:
    """The day of a click on the forecast, from its trigger value, or None."""
    return date.fromisoformat(value) if value else None
