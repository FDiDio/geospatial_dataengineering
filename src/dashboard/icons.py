"""
The weather icons of the dashboard: Meteocons by Bas Milius (MIT, github.com/basmilius/weather-icons), SVGs from
jsDelivr, one per WMO weather code by day and by night. Static in the rows of the forecast and in the charts, animated
in the headline of a day and on the map. The emoji of archive.WEATHER stay in the tables, and stand in for an icon
that does not load.
"""

import pandas as pd

from dashboard import archive

PACKAGE = "https://cdn.jsdelivr.net/npm/@bybas/weather-icons@2.0.0"
STATIC = f"{PACKAGE}/design/fill/export/wi_"
ANIMATED = f"{PACKAGE}/production/fill/all/"
SHADOW = "drop-shadow(0 1px 1.5px rgba(28, 36, 48, 0.35))"
# The icon of each code by day and by night, from the clouds of a clear sky to a thunderstorm with hail
ICONS = {
    0: ("clear-day", "clear-night"),
    1: ("partly-cloudy-day", "partly-cloudy-night"),
    2: ("overcast-day", "overcast-night"),
    3: ("overcast", "overcast"),
    45: ("fog-day", "fog-night"),
    48: ("fog-day", "fog-night"),
    51: ("partly-cloudy-day-drizzle", "partly-cloudy-night-drizzle"),
    53: ("partly-cloudy-day-drizzle", "partly-cloudy-night-drizzle"),
    55: ("drizzle", "drizzle"),
    56: ("sleet", "sleet"),
    57: ("sleet", "sleet"),
    61: ("partly-cloudy-day-rain", "partly-cloudy-night-rain"),
    63: ("rain", "rain"),
    65: ("rain", "rain"),
    66: ("sleet", "sleet"),
    67: ("sleet", "sleet"),
    71: ("partly-cloudy-day-snow", "partly-cloudy-night-snow"),
    73: ("snow", "snow"),
    75: ("snow", "snow"),
    77: ("snow", "snow"),
    80: ("partly-cloudy-day-rain", "partly-cloudy-night-rain"),
    81: ("rain", "rain"),
    82: ("rain", "rain"),
    85: ("partly-cloudy-day-snow", "partly-cloudy-night-snow"),
    86: ("snow", "snow"),
    95: ("thunderstorms-day-rain", "thunderstorms-night-rain"),
    96: ("hail", "hail"),
    99: ("hail", "hail"),
}


def url(code: float, by_day: bool = True, animated: bool = False) -> str | None:
    """The address of the icon of a WMO weather code, or None without one."""
    if pd.isna(code) or int(code) not in ICONS:
        return None
    name = ICONS[int(code)][0 if by_day else 1]
    return f"{ANIMATED}{name}.svg" if animated else f"{STATIC}{name}.svg"


def image(code: float, size: int, by_day: bool = True, animated: bool = False, shadow: bool = False) -> str:
    """
    The icon of a code as an HTML image of a size in pixels, its emoji and label as the text in its place; with a
    shadow, for its white clouds to stand out on a light page.
    """
    emoji, label = archive.sky(code, by_day)
    address = url(code, by_day, animated)
    if address is None:
        return ""
    style = f' style="filter: {SHADOW}"' if shadow else ""
    return f'<img src="{address}" width="{size}" height="{size}" alt="{emoji} {label}"{style}>'
