"""
The map of the dashboard, drawn with Leaflet through streamlit-folium. A click gives the coordinates of its point,
which the deck.gl of Streamlit did not.

The map itself never changes, so that Streamlit never draws it again: the place, its hexagon and the cells of a mart
come as layers that streamlit-folium swaps in the browser, and the view as a center and a zoom it moves the map to.
The browser draws the grid of hexagons over the whole view with h3-js, at each move, at the resolution of the zoom.
"""

import math
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import folium
import h3
from branca.element import MacroElement
from folium.elements import JSCSSMixin
from folium.template import Template

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
H3_JS = "https://cdn.jsdelivr.net/npm/h3-js@4.4.0/dist/h3-js.umd.js"

WORLD_VIEW = ((25.0, 10.0), 2.0)
# Length of the equator in meters: a Leaflet tile of 256 pixels spans it at zoom 0
EQUATOR_M = 40_075_016.686
# The grid has the finest hexagons at least this many pixels a side, about 200 to 400 of them on a desktop
GRID_EDGE_PX = 24
# The zoom on a place shows its hexagon this many pixels a side: the grid then draws hexagons of its size
PLACE_EDGE_PX = 32
# At most this many hexagons in the grid, else coarser ones
GRID_CELLS = 2500


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


class HexagonGrid(JSCSSMixin, MacroElement):
    """
    The hexagons over the whole view, drawn by the browser at each move: the finest resolution whose hexagons are
    at least GRID_EDGE_PX pixels a side, coarser beyond GRID_CELLS of them. The one under the pointer is lit, to show
    where a click goes. H3 reads a polygon wider than 180° the short way round, so the view is covered in strips,
    each within -180° and 180°, and a hexagon is drawn on the copy of the world of its strip.
    """

    _template = Template(
        """
        {% macro script(this, kwargs) %}
        (() => {
          const map = {{ this._parent.get_name() }};
          const grid = L.layerGroup().addTo(map);
          const lit = L.polygon([], {color: {{ this.lit|tojson }}, weight: 2, fillOpacity: 0.12, interactive: false})
            .addTo(map);
          let resolution = 0;
          const corners = (cell, near) =>
            h3.cellToBoundary(cell).map(([lat, lng]) => [lat, lng + 360 * Math.round((near - lng) / 360)]);
          const finest = () => {
            const metersPerPixel = {{ this.equator }} * Math.cos(map.getCenter().lat * Math.PI / 180)
              / (256 * 2 ** map.getZoom());
            let res = 0;
            while (res < 15 && h3.getHexagonEdgeLengthAvg(res + 1, "m") / metersPerPixel >= {{ this.edge }}) res++;
            return res;
          };
          const cover = (res) => {
            const bounds = map.getBounds();
            const south = Math.max(bounds.getSouth(), -85), north = Math.min(bounds.getNorth(), 85);
            const west = bounds.getWest(), east = Math.min(bounds.getEast(), west + 360);
            const cells = new Map();
            for (let start = west; start < east; start += 60) {
              const end = Math.min(start + 60, east), middle = (start + end) / 2;
              const shift = 360 * Math.floor((start + 180) / 360);
              const from = start - shift, to = end - shift;
              const pieces = to > 180 ? [[from, 180], [-180, to - 360]] : [[from, to]];
              for (const [a, b] of pieces) {
                for (const cell of h3.polygonToCells([[south, a], [south, b], [north, b], [north, a]], res)) {
                  cells.set(`${cell}@${Math.round(middle / 360)}`, [cell, middle]);
                }
                if (cells.size > {{ this.cells }}) return null;
              }
            }
            return cells;
          };
          const draw = () => {
            resolution = finest();
            let cells = cover(resolution);
            while (cells === null && resolution > 0) cells = cover(--resolution);
            grid.clearLayers();
            for (const [cell, near] of (cells || new Map()).values()) {
              const style = {color: {{ this.color|tojson }}, weight: 1, fill: false, interactive: false};
              L.polygon(corners(cell, near), style).addTo(grid);
            }
          };
          map.on("moveend", draw);
          map.on("mousemove", (event) => {
            const lng = ((event.latlng.lng + 180) % 360 + 360) % 360 - 180;
            lit.setLatLngs(corners(h3.latLngToCell(event.latlng.lat, lng, resolution), event.latlng.lng));
          });
          map.on("mouseout", () => lit.setLatLngs([]));
          draw();
        })();
        {% endmacro %}
        """
    )
    default_js: list[tuple[str, str]] = [("h3-js", H3_JS)]  # noqa: RUF012

    def __init__(self, color: str, lit: str) -> None:
        super().__init__()
        self._name = "HexagonGrid"
        self.color, self.lit = color, lit
        self.equator, self.edge, self.cells = EQUATOR_M, GRID_EDGE_PX, GRID_CELLS


def rgba(color: Sequence[int]) -> str:
    """A CSS color of [r, g, b] or [r, g, b, alpha 0-255]."""
    alpha = color[3] / 255 if len(color) > 3 else 1.0
    return f"rgba({color[0]}, {color[1]}, {color[2]}, {alpha:.2f})"


def zoom_for(resolution: int, latitude: float) -> float:
    """The zoom showing hexagons of a resolution PLACE_EDGE_PX pixels a side at a latitude, as the grid draws them."""
    meters_per_pixel = h3.average_hexagon_edge_length(resolution, unit="m") / PLACE_EDGE_PX
    return math.log2(EQUATOR_M * math.cos(math.radians(latitude)) / (256 * meters_per_pixel))


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


def base(theme: str, grid: str, lit: str, phone: bool) -> folium.Map:
    """
    The map, the same at every run: the CARTO basemap of the theme, the grid of hexagons, zoom in quarter steps,
    and the gestures of a phone.
    """
    (latitude, longitude), zoom = WORLD_VIEW
    figure = folium.Map(
        location=(latitude, longitude),
        zoom_start=zoom,  # type: ignore[arg-type]  # Leaflet takes a fraction with zoomSnap below 1
        tiles=None,
        font_size="0.75rem",  # of the controls and the credits, as Leaflet sets them
        prefer_canvas=True,
        zoom_snap=0.25,
        world_copy_jump=True,
    )
    VectorBasemap(STYLES[theme]).add_to(figure)
    HexagonGrid(grid, lit).add_to(figure)
    if phone:
        GestureHandling().add_to(figure)
    return figure


def values_layer(cells: Sequence[str], texts: Sequence[str], colors: Sequence[Sequence[int]]) -> folium.FeatureGroup:
    """The cells of a mart, colored by their value, which they show under the pointer."""
    layer = folium.FeatureGroup(name="values")
    if not cells:  # a day without events: folium refuses the tooltip of no hexagons
        return layer
    properties = {
        cell: {"value_text": text, "color": rgba(color)} for cell, text, color in zip(cells, texts, colors, strict=True)
    }
    folium.GeoJson(
        hexagons(cells, properties),
        style_function=lambda feature: {"fillColor": feature["properties"]["color"], "fillOpacity": 0.85, "weight": 0},
        tooltip=folium.GeoJsonTooltip(["value_text"], labels=False),
    ).add_to(layer)
    return layer


def place_layer(latitude: float, longitude: float, cell: str, accent: str) -> folium.FeatureGroup:
    """
    The hexagon of the place, outlined in the accent, and a white dot with a dark ring on the point: visible on
    both ends of every color scale.
    """
    layer = folium.FeatureGroup(name="place")
    style = {"color": accent, "weight": 2.5, "fillColor": accent, "fillOpacity": 0.2}
    folium.GeoJson(hexagons([cell]), style_function=lambda _: style, interactive=False).add_to(layer)
    folium.CircleMarker(
        (latitude, longitude),
        radius=6,
        color="#141414",
        weight=2,
        fill=True,
        fill_color="#ffffff",
        fill_opacity=1,
        interactive=False,
    ).add_to(layer)
    return layer


def clicked_point(state: Mapping[str, Any] | None) -> tuple[float, float] | None:
    """The point of the last click on the map, from the value st_folium keeps, or None."""
    point = (state or {}).get("last_clicked")
    return None if not point else (point["lat"], point["lng"])
