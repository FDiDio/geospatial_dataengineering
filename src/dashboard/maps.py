"""
The map of the dashboard: a Streamlit component, weather_map.js, where MapLibre draws the CARTO basemap and deck.gl
the hexagons. st.pydeck_chart tells the app the clicked object only, not the clicked point nor the view, so its
hexagons could not follow the map; the component covers the view with hexagons in the browser at each move, and a
click sends its point and the resolution of its hexagon.

The map lives as long as the page. Each run gives it its data: the place and its hexagon, the cells of a mart, and a
view, which it moves to only when its number changes.
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
CSS = ".weather-map {width: 100%; border-radius: 0.75rem; overflow: hidden}"

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
) -> dict[str, Any]:
    """
    What the map draws: the basemap of the theme, the colors of the grid and the accent, the view and its number,
    the place (latitude, longitude, hexagon) and the cells of a mart (h3, color, text).
    """
    (latitude, longitude), zoom = view
    return {
        "style": STYLES[theme],
        "grid": list(grid),
        "accent": list(accent),
        "view": {"lat": latitude, "lng": longitude, "zoom": zoom, "id": view_id},
        "height": height,
        "phone": phone,
        "place": None if place is None else {"lat": place[0], "lng": place[1], "cell": place[2]},
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
