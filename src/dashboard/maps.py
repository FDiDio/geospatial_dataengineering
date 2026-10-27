"""
The map of the dashboard, drawn with Leaflet through streamlit-folium. A click gives the coordinates of its point,
which the deck.gl of Streamlit did not, so the hexagons are drawn for their look and for the area of a place:
around a place, a patch of hexagons of the chosen size with its own outlined; with no place, a light grid on the
world or on an area; and the cells of a mart, colored, on the days of the warehouse.
"""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import folium
import h3
from branca.element import MacroElement
from folium.elements import JSCSSMixin
from folium.template import Template

# Rings of hexagons of the place's size drawn around it: a patch of 3 × 10 × 11 + 1 = 331 hexagons
RINGS = 10
# The light grid where no place is chosen: hexagons of about 1,000 km on the world, 360 km on an area
GRID_RESOLUTION = {"world": 1, "area": 2}
# The CARTO basemaps of the deck.gl maps, from their vector tiles: their raster tiles now ask for an API key and
# print "API KEY REQUIRED" across the map without one. MapLibre draws them inside Leaflet; its version 6 comes as
# an ES module only, which the plugin cannot load.
STYLES = {
    "light": "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
    "dark": "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
}
ATTRIBUTION = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors '
    '&copy; <a href="https://carto.com/attributions">CARTO</a>'
)
MAPLIBRE = "https://cdn.jsdelivr.net/npm/maplibre-gl@5.24.0/dist/maplibre-gl"
MAPLIBRE_LEAFLET = "https://cdn.jsdelivr.net/npm/@maplibre/maplibre-gl-leaflet@0.1.4/leaflet-maplibre-gl.js"
GESTURES = "https://cdn.jsdelivr.net/npm/leaflet-gesture-handling@1.2.2/dist/leaflet-gesture-handling.min"


class VectorBasemap(JSCSSMixin, MacroElement):
    """The CARTO basemap of a style, drawn by MapLibre from its vector tiles."""

    _template = Template(
        """
        {% macro script(this, kwargs) %}
            L.maplibreGL({style: {{ this.style|tojson }}, attribution: {{ this.attribution|tojson }}})
                .addTo({{ this._parent.get_name() }});
        {% endmacro %}
        """
    )
    # The files folium loads with an element, declared as it declares them
    default_js: list[tuple[str, str]] = [  # noqa: RUF012
        ("maplibre-gl", f"{MAPLIBRE}.js"),
        ("maplibre-gl-leaflet", MAPLIBRE_LEAFLET),
    ]
    default_css: list[tuple[str, str]] = [("maplibre-gl", f"{MAPLIBRE}.css")]  # noqa: RUF012

    def __init__(self, style: str) -> None:
        super().__init__()
        self._name = "VectorBasemap"
        self.style = style
        self.attribution = ATTRIBUTION


class GestureHandling(JSCSSMixin, MacroElement):
    """On a phone, one finger scrolls the page over the map and two move the map, as a hint on it says."""

    _template = Template(
        """
        {% macro script(this, kwargs) %}
            {{ this._parent.get_name() }}.gestureHandling.enable();
        {% endmacro %}
        """
    )
    default_js: list[tuple[str, str]] = [("leaflet-gesture-handling", f"{GESTURES}.js")]  # noqa: RUF012
    default_css: list[tuple[str, str]] = [("leaflet-gesture-handling", f"{GESTURES}.css")]  # noqa: RUF012


def rgba(color: Sequence[int]) -> str:
    """A CSS color of [r, g, b] or [r, g, b, alpha 0-255]."""
    alpha = color[3] / 255 if len(color) > 3 else 1.0
    return f"rgba({color[0]}, {color[1]}, {color[2]}, {alpha:.2f})"


def boundary(cell: str) -> list[list[float]]:
    """The corners of a hexagon as GeoJSON [lon, lat], closed, unbroken across the antimeridian."""
    corners = [[lon, lat] for lat, lon in h3.cell_to_boundary(cell)]
    if max(lon for lon, _ in corners) - min(lon for lon, _ in corners) > 180:
        corners = [[lon + 360 if lon < 0 else lon, lat] for lon, lat in corners]
    return [*corners, corners[0]]


def hexagons(cells: Iterable[str], properties: Mapping[str, Mapping[str, Any]] | None = None) -> dict:
    """A GeoJSON FeatureCollection of hexagons, with the properties of each."""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [boundary(cell)]},
                "properties": {"h3": cell, **(properties or {}).get(cell, {})},
            }
            for cell in cells
        ],
    }


def grid_cells(bbox: tuple[float, float, float, float] | None, resolution: int) -> list[str]:
    """The hexagons of a resolution over a bounding box (lat_min, lat_max, lon_min, lon_max), or the world."""
    if bbox is None:
        return [child for cell in h3.get_res0_cells() for child in h3.cell_to_children(cell, resolution)]
    lat_min, lat_max, lon_min, lon_max = bbox
    lon_min, lon_max = max(lon_min, -180.0), min(lon_max, 180.0)
    outline = [(lat_min, lon_min), (lat_min, lon_max), (lat_max, lon_max), (lat_max, lon_min)]
    return list(h3.polygon_to_cells(h3.LatLngPoly(outline), resolution))


def base(center: tuple[float, float], zoom: float, theme: str, phone: bool) -> folium.Map:
    """The map: the CARTO basemap of the theme, zoom in quarter steps, and the gestures of a phone."""
    figure = folium.Map(
        location=center,
        zoom_start=zoom,  # type: ignore[arg-type]  # Leaflet takes a fraction with zoomSnap below 1
        tiles=None,
        font_size="0.75rem",  # of the controls and the credits, as Leaflet sets them
        prefer_canvas=True,
        zoom_snap=0.25,
        world_copy_jump=True,
    )
    VectorBasemap(STYLES[theme]).add_to(figure)
    if phone:
        GestureHandling().add_to(figure)
    return figure


def add_grid(figure: folium.Map, cells: Iterable[str], color: str) -> None:
    """Hexagons outlined lightly, which let a click through to the map."""
    style = {"color": color, "weight": 1, "fill": False}
    folium.GeoJson(hexagons(cells), style_function=lambda _: style, interactive=False).add_to(figure)


def add_values(figure: folium.Map, cells: Sequence[str], texts: Sequence[str], colors: Sequence[Sequence[int]]) -> None:
    """The cells of a mart, colored by their value, which they show under the pointer."""
    if not cells:  # a day without events: folium refuses the tooltip of no hexagons
        return
    properties = {
        cell: {"value_text": text, "color": rgba(color)} for cell, text, color in zip(cells, texts, colors, strict=True)
    }
    folium.GeoJson(
        hexagons(cells, properties),
        style_function=lambda feature: {"fillColor": feature["properties"]["color"], "fillOpacity": 1, "weight": 0},
        tooltip=folium.GeoJsonTooltip(["value_text"], labels=False),
    ).add_to(figure)


def add_place(figure: folium.Map, latitude: float, longitude: float, cell: str, accent: str) -> None:
    """
    The hexagon of the place, outlined in the accent, and a white dot with a dark ring on the point: visible on
    both ends of every color scale.
    """
    style = {"color": accent, "weight": 2, "fillColor": accent, "fillOpacity": 0.2}
    folium.GeoJson(hexagons([cell]), style_function=lambda _: style, interactive=False).add_to(figure)
    folium.CircleMarker(
        (latitude, longitude),
        radius=6,
        color="#141414",
        weight=2,
        fill=True,
        fill_color="#ffffff",
        fill_opacity=1,
        interactive=False,
    ).add_to(figure)


def clicked_point(state: Mapping[str, Any] | None) -> tuple[float, float] | None:
    """The point of the last click on the map, from the value st_folium keeps, or None."""
    point = (state or {}).get("last_clicked")
    return None if not point else (point["lat"], point["lng"])
