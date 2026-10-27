"""
The map of the dashboard: a Streamlit component, weather_map.js, where MapLibre draws the CARTO basemap and deck.gl
the hexagons. st.pydeck_chart tells the app the clicked object only, not the clicked point nor the view, so its
hexagons could not follow the map; the component covers the view with hexagons in the browser at each move, and a
click sends its point and the resolution of its hexagon.

The map lives as long as the page. Each run gives it its data: the place and its hexagon, the cells of a mart, and a
view, which it moves to only when its number changes. A popup on the place sums up its weather, with a button down to
the card of the place (key "place" in explorer.py): below the map, people did not see it.
"""

import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import h3
import streamlit as st

# The CARTO basemaps, from their vector tiles: their raster tiles now ask for an API key and print "API KEY
# REQUIRED" across the map without one
STYLES = {
    "light": "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    "dark": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
}
# The world at the start, its hexagons about 360 km across, a dozen pixels
WORLD_VIEW = ((25.0, 10.0), 1.3)
# MapLibre draws the equator, this long in meters, 512 pixels wide at zoom 0
EQUATOR_M = 40_075_016.686
TILE_PX = 512
# A hexagon size zooms the map to show those hexagons this many pixels a side: the browser then draws them, its
# finest at least 3 + 1.2 × zoom pixels a side (weather_map.js)
PLACE_EDGE_PX = 18
# The popup of the place, in the colors of the theme, its tip of the color of its box on whichever side it points,
# over the canvas of deck.gl
CSS = """
.weather-map {width: 100%; border-radius: 0.75rem; overflow: hidden}
.weather-map .maplibregl-popup {z-index: 3}
.weather-popup .maplibregl-popup-content {
  padding: 10px 14px 12px; border-radius: 10px; font: 14px/1.4 "Source Sans Pro", sans-serif;
  box-shadow: 0 4px 16px rgba(0, 0, 0, 0.25);
}
.weather-popup .title {font-weight: 700; font-size: 15px; padding-right: 14px}
.weather-popup .line {margin-top: 2px}
.weather-popup .muted {opacity: 0.7; font-size: 13px}
.weather-popup button.details {
  margin-top: 8px; width: 100%; padding: 6px 10px; border: none; border-radius: 6px; color: #fff;
  font: 600 14px "Source Sans Pro", sans-serif; cursor: pointer;
}
.weather-popup .maplibregl-popup-close-button {font-size: 18px; padding: 0 6px; color: inherit}
.weather-popup.dark .maplibregl-popup-content {background: #262730; color: #fafafa}
.weather-popup.dark[class*="anchor-top"] .maplibregl-popup-tip {border-bottom-color: #262730}
.weather-popup.dark[class*="anchor-bottom"] .maplibregl-popup-tip {border-top-color: #262730}
.weather-popup.dark.maplibregl-popup-anchor-left .maplibregl-popup-tip {border-right-color: #262730}
.weather-popup.dark.maplibregl-popup-anchor-right .maplibregl-popup-tip {border-left-color: #262730}
"""

MAP = st.components.v2.component(
    "weather_map",
    js=Path(__file__).with_name("weather_map.js").read_text(encoding="utf-8"),
    css=CSS,
    # In the page, where MapLibre finds its stylesheet
    isolate_styles=False,
)


def zoom_for(resolution: int, latitude: float) -> float:
    """The zoom showing hexagons of a resolution PLACE_EDGE_PX pixels a side at a latitude."""
    meters_per_pixel = h3.average_hexagon_edge_length(resolution, unit="m") / PLACE_EDGE_PX
    return math.log2(EQUATOR_M * math.cos(math.radians(latitude)) / (TILE_PX * meters_per_pixel))


def data(
    theme: str,
    grid: Sequence[int],
    accent: Sequence[int],
    view: tuple[tuple[float, float], float],
    view_id: int,
    height: int,
    phone: bool,
    place: tuple[float, float, str] | None = None,
    values: Sequence[Mapping[str, Any]] = (),
    popup: tuple[str, Sequence[str]] = ("", ()),
) -> dict[str, Any]:
    """
    What the map draws: the basemap of the theme, the colors of the grid and the accent, the view and its number,
    the place (latitude, longitude, hexagon) with the title and lines of its popup, and the cells of a mart (h3,
    color, text).
    """
    (latitude, longitude), zoom = view
    title, lines = popup
    return {
        "style": STYLES[theme],
        "dark": theme == "dark",
        "grid": list(grid),
        "accent": list(accent),
        "view": {"lat": latitude, "lng": longitude, "zoom": zoom, "id": view_id},
        "height": height,
        "phone": phone,
        "place": None
        if place is None
        else {"lat": place[0], "lng": place[1], "cell": place[2], "title": title, "lines": list(lines)},
        "values": list(values),
    }


def show(map_data: Mapping[str, Any], key: str, on_click: Any) -> Any:
    """The map, keyed to keep it over the runs; on_click runs before the run a click starts."""
    return MAP(key=key, data=map_data, on_click_change=on_click)


def clicked(value: Mapping[str, Any] | None) -> tuple[float, float, int] | None:
    """The point and the resolution of the hexagon of a click on the map, from its trigger value, or None."""
    if not value:
        return None
    return float(value["lat"]), float(value["lng"]), int(value["resolution"])
