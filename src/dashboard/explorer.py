"""
The explorer page of the dashboard, app.py: a map of the dbt marts on H3 hexagons, and the weather of any
place on demand.

The map shows the days ingested in the warehouse DUCKDB_PATH, by default output/warehouse.duckdb.
The weather of a place, searched by name or coordinates or clicked on the map, on any day since 1940,
comes from the ERA5 archive of Open-Meteo, the altitude of its H3 cell from the Copernicus elevation model.
"""

import calendar
import math
import os
import sys
from datetime import date

import altair as alt
import duckdb
import h3
import pandas as pd
import pydeck as pdk
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import archive, places, queries, terrain
from dashboard.queries import DEFAULT_WAREHOUSE, LAYERS, REGIONS, colorize, scale_colors

WORLD = "World (H3 regions, ~12,000 km²)"
SERIES_COLOR = {"light": "#2a78d6", "dark": "#3987e5"}
# The second series of a chart, the gusts beside the wind: orange against the blue, dashed besides
GUST_COLOR = {"light": "#c4501f", "dark": "#f08a5f"}
# The sunshine, in amber, and the clouds, in gray
SUN_COLOR = {"light": "#d99a06", "dark": "#f5b82e"}
CLOUD_COLOR = {"light": "#9aa5b1", "dark": "#6b7684"}
# The accent of the map, as RGB: the hexagon under the pointer and the one of the place
ACCENT = {"light": [42, 120, 214], "dark": [57, 135, 229]}
# Outlines of the clickable hexagons, and the muted ink and grid lines of the charts
GRID = {"light": [40, 50, 70, 45], "dark": [220, 225, 235, 40]}
MUTED = {"light": "#5b6573", "dark": "#a3acb9"}
CHART_GRID = {"light": "#e9ecf0", "dark": "#273039"}
# Zoom on a place: its 20 km hexagons about 60 pixels across, easy to click. Each finer resolution of
# hexagons is √7 times smaller, so the map zooms in by log2(√7) levels to keep them as large.
PLACE_ZOOM = 7.5
ZOOM_PER_RESOLUTION = math.log2(math.sqrt(7))
# Width of a metric tile, in pixels: tiles of a fixed width wrap onto new rows in a narrow window, where
# columns would squeeze them and Streamlit would cut their labels and values short
TILE_WIDTH = 220
MONTHS = list(calendar.month_abbr)[1:]
# What a click on a clickable hexagon does: the finest ones around a place give its weather, the coarser ones
# beyond take the map there, among fine ones
WEATHER, GO = "click for its weather", "click to go there"

theme = getattr(getattr(st.context, "theme", None), "type", None) or "light"
language = getattr(st.context, "locale", None) or "en"
warehouse = os.environ.get("DUCKDB_PATH", DEFAULT_WAREHOUSE)


@st.cache_data(ttl=300, show_spinner=False)
def load(path: str, query: str, *args):
    """
    Run a dashboard query on a short-lived read-only connection, caching the result.

    DuckDB lets a single process write a database file: a dashboard holding a connection
    open would block the dbt runs, so the connection is closed after every query.
    """
    with queries.connect(path) as con:
        return getattr(queries, query)(con, *args)


@st.cache_data(ttl=86400, show_spinner=False)
def search_places(query: str, language: str) -> list[places.Place]:
    """Places matching a name, cached: the geocoder allows one request per second."""
    return places.search(query, language)


@st.cache_data(ttl=86400, show_spinner=False)
def name_of(latitude: float, longitude: float, language: str) -> str | None:
    return places.reverse(latitude, longitude, language)


@st.cache_data(ttl=86400, show_spinner="Loading the weather of the place…")
def load_reanalysis(latitude: float, longitude: float, period: archive.Period, fields: tuple) -> archive.Weather:
    """
    ERA5 for a place. The fields fetched are part of the cache key: a server still running when they
    change does not serve its cached weather without them.
    """
    return archive.fetch(latitude, longitude, *period, archive.ERA5)


@st.cache_data(ttl=3600, show_spinner="Loading the forecast of the place…")
def load_operational(latitude: float, longitude: float, period: archive.Period, fields: tuple) -> archive.Weather:
    """ECMWF IFS for a place, its forecast renewed a few times a day."""
    return archive.fetch(latitude, longitude, *period, archive.IFS)


def load_weather(latitude: float, longitude: float, year: int) -> tuple[archive.Weather | None, list[str]]:
    """
    Weather of a place over a year: ERA5, then ECMWF IFS up to the end of its forecast, with the errors of
    the sources that failed. Each source is cached on its own: one failing does not hide the other, and the
    failure is not cached, so that the next run asks again.
    """
    now = pd.Timestamp.now(tz="UTC")
    era5, ifs = archive.periods(date(year, 1, 1), min(date(year, 12, 31), archive.last_forecast_day()), now)
    weathers: dict[str, archive.Weather] = {}
    errors = []
    for source, period, loader in [(archive.ERA5, era5, load_reanalysis), (archive.IFS, ifs, load_operational)]:
        if period is None:
            continue
        try:
            weathers[source.name] = loader(latitude, longitude, period, tuple(archive.HOURLY))
        except (OSError, ValueError) as error:
            errors.append(f"{source.name}: {error}")
    if not weathers:
        return None, errors
    return archive.combine(weathers.get(archive.ERA5.name), weathers.get(archive.IFS.name), now), errors


@st.cache_data(show_spinner=False)
def cell_altitude(cell: str) -> terrain.Altitude:
    return terrain.cell_altitude(cell)


@st.cache_data(show_spinner=False)
def pick_cells(bbox: tuple[float, float, float, float] | None, resolution: int, hint: str = GO) -> pd.DataFrame:
    return queries.pick_cells(bbox, resolution, hint)


def measure(value: float | None, unit: str) -> str:
    return "–" if pd.isna(value) else f"{value:.1f} {unit}"


def styled(chart, height: int, title: str):
    """A chart in the look of the dashboard: its title on the left, no frame, light grid lines."""
    return (
        chart.properties(height=height, title=alt.TitleParams(title, anchor="start", fontSize=14, offset=12))
        .configure_view(stroke=None)
        .configure_axis(
            domain=False,
            ticks=False,
            labelPadding=6,
            labelColor=MUTED[theme],
            titleColor=MUTED[theme],
            gridColor=CHART_GRID[theme],
        )
    )


def from_zero(values: pd.Series, minimum: float = 1.0) -> alt.Scale:
    """A scale from 0 to at least the minimum: on a dry day, the precipitation would get a scale of no range."""
    top = values.max()
    return alt.Scale(domain=[0, max(minimum, 0.0 if pd.isna(top) else float(top))], nice=True)


def tile(label: str, value: str, **kwargs) -> None:
    """A metric in a bordered tile of TILE_WIDTH."""
    st.metric(label, value, border=True, width=TILE_WIDTH, **kwargs)


# The average of the month each value of a day compares to
STATISTIC_OF_DAY = {
    "temperature_max_c": "temperature_max_mean_c",
    "temperature_min_c": "temperature_min_mean_c",
    "temperature_mean_c": "temperature_mean_c",
}


def versus(value: float, month: pd.Series | None, statistic: str) -> str | None:
    """The difference of a value of a day to the average of its month, as the delta of a metric."""
    if month is None or pd.isna(value) or pd.isna(month[statistic]):
        return None
    return f"{value - month[statistic]:+.1f} °C vs month"


def zoom_by(step: float) -> None:
    st.session_state["zoom"] = min(max(st.session_state["zoom"] + step, 0.0), 18.0)


def recenter() -> None:
    st.session_state["recenter"] = True


def show_statistics(statistics: pd.Series) -> None:
    """The averages, extremes and totals of a period, as tiles."""
    with st.container(horizontal=True):
        tile("Average high", measure(statistics["temperature_max_mean_c"], "°C"))
        tile("Average low", measure(statistics["temperature_min_mean_c"], "°C"))
        tile("Average", measure(statistics["temperature_mean_c"], "°C"))
        tile("Highest", measure(statistics["temperature_max_c"], "°C"))
        tile("Lowest", measure(statistics["temperature_min_c"], "°C"))
        tile("Total precipitation", measure(statistics["precipitation_mm"], "mm"))
        tile(
            "Wet days",
            f"{statistics['wet_days']} of {statistics['days']}",
            help=f"Days with at least {archive.WET_DAY_MM:g} mm of precipitation",
        )
        tile("Average wind", measure(statistics["wind_speed_mean_kmh"], "km/h"), help=WIND_HELP)
        tile("Strongest gust", measure(statistics["wind_gusts_max_kmh"], "km/h"))
        tile("Total sunshine", measure(statistics["sunshine_h"], "h"), help=SUNSHINE_HELP)
        tile("Average humidity", measure(statistics["humidity_mean_pct"], "%"), help=HUMIDITY_HELP)


def show_period_charts(
    data: pd.DataFrame, x: str, domain: list, selected: object, low: str, high: str, tooltip: list, titles: list[str]
) -> None:
    """
    Charts of a period, one column per day or month whether it has data or not, the selected one marked
    by a dashed rule: a bar from the low to the high temperature, under the icon of the weather of each day
    when the data has one, the precipitation, the mean wind and the sunshine.
    """
    axis = alt.X(
        f"{x}:O",
        title=None,
        scale=alt.Scale(domain=domain, paddingInner=0.35),
        axis=alt.Axis(labelAngle=0, labelOverlap=True),
    )
    marker = alt.Chart(pd.DataFrame({x: [selected]})).mark_rule(strokeDash=[4, 4], opacity=0.5).encode(x=f"{x}:O")
    temperature = (
        alt.Chart(data)
        .mark_bar(color=SERIES_COLOR[theme], cornerRadius=4)
        .encode(
            x=axis,
            # Room above the highest bar for its icon
            y=alt.Y(f"{low}:Q", title="°C", scale=alt.Scale(zero=False, padding=20 if "icon" in data else 0)),
            y2=f"{high}:Q",
            tooltip=tooltip,
            **by_source(data, legend=True),
        )
    )
    if "icon" in data:
        icons = alt.Chart(data).mark_text(dy=-14, fontSize=15).encode(x=axis, y=f"{high}:Q", text="icon:N")
        temperature = temperature + icons
    st.altair_chart(styled(temperature + marker, 240, titles[0]), width="stretch")
    precipitation = (
        alt.Chart(data)
        .mark_bar(color=SERIES_COLOR[theme], cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=axis,
            y=alt.Y("precipitation_mm:Q", title="mm", scale=from_zero(data["precipitation_mm"])),
            tooltip=tooltip,
            **by_source(data, legend=False),
        )
    )
    st.altair_chart(styled(precipitation + marker, 160, titles[1]), width="stretch")
    wind = (
        alt.Chart(data)
        .mark_bar(color=SERIES_COLOR[theme], cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=axis, y=alt.Y("wind_speed_mean_kmh:Q", title="km/h"), tooltip=tooltip, **by_source(data, legend=False)
        )
    )
    st.altair_chart(styled(wind + marker, 160, titles[2]), width="stretch")
    sunshine = (
        alt.Chart(data)
        .mark_bar(color=SUN_COLOR[theme], cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=axis,
            y=alt.Y("sunshine_h:Q", title="h", scale=from_zero(data["sunshine_h"])),
            tooltip=tooltip,
            **by_source(data, legend=False),
        )
    )
    st.altair_chart(styled(sunshine + marker, 160, titles[3]), width="stretch")


SOURCE_BADGES = {
    archive.ERA5.name: ":green-badge[:material/verified: ERA5 reanalysis]",
    archive.IFS.name: ":blue-badge[:material/update: ECMWF IFS · not in ERA5 yet]",
    archive.FORECAST: ":orange-badge[:material/online_prediction: Forecast · ECMWF IFS]",
}
# The sources of the days in the charts: the same hue, lighter for the last days and lighter still for the
# forecast. Vega-Lite spreads an opacity over the sources evenly between the two ends of its range.
SOURCE_OPACITY = alt.Scale(domain=archive.SOURCES, range=[1.0, 0.3])


def by_source(data: pd.DataFrame, legend: bool) -> dict:
    """The encoding of the source of each day of a chart, when its days come from more than one."""
    if "source" not in data or data["source"].nunique() < 2:
        return {}
    return {
        "opacity": alt.Opacity(
            "source:N", scale=SOURCE_OPACITY, legend=alt.Legend(title=None, orient="top") if legend else None
        )
    }


TEMPERATURE_COLUMN = st.column_config.NumberColumn(format="%.1f °C")
PRECIPITATION_COLUMN = st.column_config.NumberColumn(format="%.1f mm")
WIND_COLUMN = st.column_config.NumberColumn(format="%.0f km/h")
SUNSHINE_COLUMN = st.column_config.NumberColumn(format="%.1f h")
PERCENT_COLUMN = st.column_config.NumberColumn(format="%.0f %%")
WIND_HELP = "Wind at 10 m above the ground, the mean of the hourly speeds"
SUNSHINE_HELP = "Hours with direct sunlight, of more than 120 W/m² as the WMO defines sunshine"
HUMIDITY_HELP = "Relative humidity at 2 m above the ground, the mean of the hourly values"

try:
    dates = load(warehouse, "available_dates")
except duckdb.Error:  # no warehouse yet
    dates = []

# A click on the map selects the invisible hexagon under it: its center becomes the place. While a selection
# is active, Streamlit dims every other object of the map, and a selection counts only while its layer is on
# the map: the layers are renamed after each click to clear it. Remounting the map with a new key would clear
# it too, but each mount takes a WebGL context, and the browser blanks the map after a dozen.
clicks = st.session_state.setdefault("map_clicks", 0)
clicked = places.clicked_point(
    st.session_state.get("map"), [f"pick-{clicks}", f"near-{clicks}", f"weather-{clicks}", f"world-{clicks}"]
)
if clicked is not None:
    st.session_state["place_query"] = places.format_coordinates(*clicked)
    st.session_state["map_clicks"] = clicks = clicks + 1

with st.sidebar:
    st.header(":material/public: ERA5 weather")
    place = None
    place_query = st.text_input(
        "Place",
        key="place_query",
        placeholder="A city or coordinates, e.g. Milano or 45.46, 9.19",
        help="A name, looked up on OpenStreetMap (Nominatim), or latitude and longitude in degrees. "
        "You can also click the map.",
    ).strip()
    coordinates = places.parse_coordinates(place_query) if place_query else None
    if coordinates is not None:
        place = places.at(*coordinates)
    elif place_query:
        matches: list[places.Place] | None
        try:
            matches = search_places(place_query, language)
        except (OSError, ValueError) as error:
            st.warning(f"Place search unavailable: {error}")
            matches = None
        if matches == []:
            st.warning(f"No place found for “{place_query}”.")
        elif matches:
            place = matches[0] if len(matches) == 1 else st.selectbox("Matches", matches, format_func=lambda p: p.label)
            if place is not None:
                st.caption(f":material/location_on: {place.latitude:.6f}, {place.longitude:.6f}")
    day = st.date_input(
        "Day",
        value=dates[-1] if dates else date.today(),
        min_value=archive.FIRST_DAY,
        max_value=archive.last_forecast_day(),
        help="A place has the weather of any day since 1940 and the forecast of the next two weeks; "
        "the map has colors on the ingested days",
    )
    if dates:
        layer_key = st.radio("Layer", list(LAYERS), format_func=lambda key: LAYERS[key].label)
        layer = LAYERS[layer_key]
    area = st.selectbox(
        "Area",
        [WORLD, *REGIONS],
        disabled=place is not None,
        help="Where the map looks when no place is chosen",
    )
    bbox = queries.place_window(place.latitude, place.longitude) if place else REGIONS.get(area)
    st.caption(
        "Weather: ERA5 reanalysis (Copernicus), then ECMWF IFS for the last days and the forecast, via "
        "Open-Meteo · Altitude: Copernicus DEM 90 m · Places: © OpenStreetMap contributors"
    )

st.title("ERA5 weather explorer")
st.caption(
    "Search a place, type its coordinates or click a hexagon of the map: its weather on any day since 1940, "
    "and its forecast for the next two weeks."
)

on_map = day in dates
# The size of the hexagons around a place, chosen above the map: read before its widget, from the last run
resolution = st.session_state.get("hexagons") or queries.PLACE_RESOLUTION
if dates and not on_map:
    st.caption(f"The map has colors on the ingested days only, from {dates[0]:%d %b %Y} to {dates[-1]:%d %b %Y}.")

if on_map:
    kpis = load(warehouse, "headline", day)
    with st.container(horizontal=True):
        tile("Global mean temperature", measure(kpis["global_mean_c"], "°C"))
        tile("Hottest cell", measure(kpis["hottest_c"], "°C"))
        tile("Coldest cell", measure(kpis["coldest_c"], "°C"))
        tile("Cells in a heatwave", f"{kpis['heatwave_cells']:,}")
        tile("Extreme rain cells", f"{kpis['extreme_precipitation_cells']:,}")

    values = load(
        warehouse, "layer_values", day, layer_key, bbox, resolution if place else queries.ZOOM_RESOLUTION
    ).copy()
    values["color"] = colorize(values["value"], layer, theme)
    values["value_text"] = values["value"].map(lambda v: f"{v:,.1f} {layer.unit}")

# The hexagon of the place, at the resolution of the clickable ones around it
cell = h3.latlng_to_cell(place.latitude, place.longitude, resolution) if place else None

# The clickable hexagons, each layer over the coarser ones: the whole world in coarse ones, so that a click
# anywhere is taken even zoomed out; around a place, hexagons two sizes coarser than its own on a window 7 times
# as large, a click on which moves the place within half of one of them, among fine ones to pick the point
# exactly; and the fine ones. These cannot follow the view: deck.gl tells Streamlit the clicked hexagon, not the
# clicked point nor where the map is.
pickables = [(f"world-{clicks}", pick_cells(None, queries.PICK_RESOLUTION["world"], GO))]
if place is not None:
    center, zoom = (place.latitude, place.longitude), PLACE_ZOOM + ZOOM_PER_RESOLUTION * (resolution - 5)
    near = resolution - 2
    if near > queries.PICK_RESOLUTION["world"]:
        pickables.append((f"near-{clicks}", pick_cells(queries.pick_window(*center, near), near, GO)))
    pickables.append((f"pick-{clicks}", pick_cells(queries.pick_window(*center, resolution), resolution, WEATHER)))
elif bbox is None:
    center, zoom = (25.0, 10.0), 1.0
else:
    lat_min, lat_max, lon_min, lon_max = bbox
    center, zoom = ((lat_min + lat_max) / 2, (lon_min + lon_max) / 2), 3.5
    pickables.append((f"pick-{clicks}", pick_cells(bbox, queries.PICK_RESOLUTION["area"], GO)))

# Streamlit moves the map only by the values of its initial view that changed, keeping the view of the user
# otherwise: a new place, area or hexagon size, or the Recenter button, nudges all of them, so that the map
# moves there again; the zoom buttons change the zoom alone, around what the map shows
if (place, bbox, resolution) != st.session_state.get("viewed") or st.session_state.pop("recenter", False):
    st.session_state["viewed"] = (place, bbox, resolution)
    st.session_state["views"] = st.session_state.get("views", 0) + 1
    st.session_state["zoom"] = zoom
nudge = st.session_state["views"] % 2 * 1e-6
view = pdk.ViewState(latitude=center[0] + nudge, longitude=center[1] + nudge, zoom=st.session_state["zoom"] + nudge)

accent = ACCENT[theme]
layers = [
    # Nearly transparent (fully transparent objects are not pickable) and lit under the pointer; the finest
    # ones outlined, to show where a click goes. Line widths are clamped in pixels, as for the dot below.
    pdk.Layer(
        "H3HexagonLayer",
        cells,
        id=layer_id,
        get_hexagon="h3",
        get_fill_color=[128, 128, 128, 1],
        extruded=False,
        stroked=layer_id.startswith("pick") or (place is None and bbox is None),
        get_line_color=GRID[theme],
        get_line_width=1,
        line_width_min_pixels=1,
        line_width_max_pixels=1,
        pickable=True,
        auto_highlight=True,
        # The world's hexagons, hundreds of km across, lit lightly: they would cover the map
        highlight_color=[*accent, 50 if layer_id.startswith("world") and place is not None else 120],
    )
    for layer_id, cells in pickables
]
if on_map:
    layers.append(
        pdk.Layer(
            "H3HexagonLayer",
            values[["h3", "value_text", "color"]],
            id=f"weather-{clicks}",
            get_hexagon="h3",
            get_fill_color="color",
            extruded=False,
            stroked=False,
            pickable=True,
            auto_highlight=True,
            highlight_color=[*accent, 120],
        )
    )
if place is not None:
    # The hexagon of the place, outlined in the accent, and a white dot with a dark ring on the place: visible
    # on both ends of every color scale. Sizes are clamped in pixels: the deck.gl of Streamlit draws a radius
    # in "pixels" units as large as the whole map.
    layers.append(
        pdk.Layer(
            "H3HexagonLayer",
            [{"h3": cell}],
            id="cell",
            get_hexagon="h3",
            get_fill_color=[*accent, 50],
            get_line_color=[*accent, 255],
            get_line_width=2,
            line_width_min_pixels=2,
            line_width_max_pixels=2,
            extruded=False,
            stroked=True,
            pickable=False,
        )
    )
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            [{"position": [place.longitude, place.latitude]}],
            id="place",
            get_position="position",
            get_radius=6,
            radius_min_pixels=6,
            radius_max_pixels=6,
            get_fill_color=[255, 255, 255],
            get_line_color=[20, 20, 20],
            get_line_width=2,
            line_width_min_pixels=2,
            line_width_max_pixels=2,
            stroked=True,
            pickable=False,
        )
    )

tooltip_style = {
    "backgroundColor": "#1c2430" if theme == "light" else "#e6e9ee",
    "color": "#ffffff" if theme == "light" else "#1c2430",
    "fontSize": "12px",
    "padding": "6px 10px",
    "borderRadius": "8px",
}
# The toolbar of the map, on the left: Streamlit shows the toolbar of the chart over its top right corner
with st.container(horizontal=True, vertical_alignment="bottom", gap="medium"):
    st.segmented_control(
        "Hexagons",
        queries.PLACE_RESOLUTIONS,
        default=queries.PLACE_RESOLUTION,
        required=True,
        format_func=queries.hexagon_size,
        key="hexagons",
        disabled=place is None,
        help="Size of the hexagon of a place and of the clickable ones around it, from corner to corner. "
        "The altitude is of the hexagon; the weather comes from the 28 km (0.25°) grid of ERA5 whatever the size.",
    )
    with st.container(horizontal=True, width="content"):
        st.button("Zoom out", icon=":material/zoom_out:", on_click=zoom_by, args=(-1.0,))
        st.button("Zoom in", icon=":material/zoom_in:", on_click=zoom_by, args=(1.0,))
        st.button("Recenter", icon=":material/my_location:", on_click=recenter)

st.pydeck_chart(
    pdk.Deck(
        layers=layers,
        initial_view_state=view,
        # The layers on every copy of the world the basemap repeats when zoomed out, clickable on each
        views=[pdk.View(type="MapView", controller=True, repeat=True)],
        map_provider="carto",
        map_style="dark" if theme == "dark" else "light",
        tooltip={"html": f"<b>{layer.label}</b><br/>{{value_text}}<br/><small>{{h3}}</small>", "style": tooltip_style}
        if on_map
        else {"text": "{value_text}", "style": tooltip_style},
    ),
    height=580,
    on_select="rerun",
    selection_mode="single-object",
    key="map",
)
# The zoom buttons of the map pass their clicks on to the hexagons under them, which would move the place
ZOOM_BUTTONS = 'div:has(> button[title="Zoom In"]), button[title="Zoom In"], button[title="Zoom Out"]'
st.html(f"<style>{ZOOM_BUTTONS} {{display: none}}</style>")
st.caption(
    ":material/touch_app: Click a hexagon for its weather. Away from the place, a click takes the map there, "
    "among fine hexagons: a second click picks the point. Zoom with the mouse wheel or two fingers."
)

if on_map:
    # Legend: the color scale of the layer with its bounds and, for diverging scales, its midpoint
    gradient = ", ".join(scale_colors(layer, theme))
    labels = [f"{layer.low:g}", f"{layer.center:g}", f"{layer.high:g}"] if layer.scale == "diverging" else [
        f"{layer.low:g}", f"{(layer.low + layer.high) / 2:g}", f"≥ {layer.high:g}"
    ]  # fmt: skip
    st.markdown(
        f"""
        <div style="max-width:420px">
          <div style="font-size:0.85rem">{layer.label} ({layer.unit})</div>
          <div style="height:12px;border-radius:4px;background:linear-gradient(to right, {gradient})"></div>
          <div style="display:flex;justify-content:space-between;font-size:0.8rem;opacity:0.75">
            {"".join(f"<span>{label}</span>" for label in labels)}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if layer.events and values.empty:
        st.caption("No cell is affected on this day.")

if place is not None and cell is not None:
    weather, weather_errors = load_weather(place.latitude, place.longitude, day.year)
    altitude: terrain.Altitude | None
    try:
        altitude = cell_altitude(cell)
    except (OSError, ValueError, KeyError):
        altitude = None

    with st.container(border=True):
        st.subheader(place.short_name)
        if place.kind == "coordinates":
            try:
                near = name_of(place.latitude, place.longitude, language)
            except (OSError, ValueError):
                near = None
            st.caption(f"Near {near}" if near else "No named place here.")
        else:
            st.caption(place.name)
        # The exact coordinates of the place, in decimal degrees to copy (a code block has a copy button)
        # and in degrees, minutes and seconds
        with st.container(horizontal=True, vertical_alignment="center"):
            st.code(f"{place.latitude:.6f}, {place.longitude:.6f}", language=None, width="content")
            st.markdown(f":blue-badge[:material/location_on: {places.format_dms(place.latitude, place.longitude)}]")
        today = None
        if weather is not None:
            daily, hourly = weather.daily, weather.hourly
            # The statistics of the month and the year are of the days gone by: the forecast is left out
            past = daily[daily["source"] != archive.FORECAST]
            selected = daily[daily["date"].dt.date == day]
            published = hourly[hourly["time"].dt.date == day]
            month_days = daily[daily["date"].dt.month == day.month]
            month_past = past[past["date"].dt.month == day.month]
            month = None if month_past.empty else archive.summary(month_past)
            today = None if selected.empty else selected.iloc[0]

        badges = [f":gray-badge[:material/hexagon: H3 {cell} · {h3.cell_area(cell, 'km^2'):,.0f} km²]"]
        if weather is not None:
            badges.append(f":gray-badge[:material/schedule: {weather.timezone}]")
        if today is not None:
            badges.insert(0, SOURCE_BADGES[today["source"]])
        st.markdown(" ".join(badges))
        if today is not None:
            icon, label = archive.sky(today["weather_code"])
            st.markdown(f"### {icon} {label}")

        if weather_errors:
            missing = "Weather of the place unavailable" if weather is None else "Part of the weather is missing"
            st.warning(f"{missing}, try again in a moment. {'; '.join(weather_errors)}")
        if weather is not None and selected.empty and published.empty:
            st.info("No data of this place on this day yet.")
        elif weather is not None and selected.empty:
            st.info(
                f"This day is not complete yet: the data reach {published['time'].max():%H:%M} here, so its "
                "high, low and precipitation would be wrong. Its hours so far are in the Day tab."
            )

        with st.container(horizontal=True):
            if today is not None:
                for label, column in [
                    ("High", "temperature_max_c"),
                    ("Low", "temperature_min_c"),
                    ("Average", "temperature_mean_c"),
                ]:
                    tile(
                        label,
                        measure(today[column], "°C"),
                        delta=versus(today[column], month, STATISTIC_OF_DAY[column]),
                        delta_color="off",
                    )
                tile("Precipitation", measure(today["precipitation_mm"], "mm"))
                gust = today["wind_gusts_max_kmh"]
                tile(
                    "Wind",
                    measure(today["wind_speed_mean_kmh"], "km/h"),
                    delta=None
                    if pd.isna(gust)
                    else f"from {archive.compass(today['wind_direction_deg'])} · gusts {gust:.0f} km/h",
                    delta_color="off",
                    delta_arrow="off",
                    help=f"{WIND_HELP}, with the direction it mostly blows from and the strongest gust",
                )
                clouds = today["cloud_cover_mean_pct"]
                tile(
                    "Sunshine",
                    measure(today["sunshine_h"], "h"),
                    delta=None if pd.isna(clouds) else f"clouds {clouds:.0f} %",
                    delta_color="off",
                    delta_arrow="off",
                    help=f"{SUNSHINE_HELP}, and the mean cloud cover of the day",
                )
                tile(
                    "Humidity",
                    measure(today["humidity_mean_pct"], "%"),
                    delta=f"{today['humidity_min_pct']:.0f}–{today['humidity_max_pct']:.0f} %",
                    delta_color="off",
                    delta_arrow="off",
                    help=f"{HUMIDITY_HELP}, with the lowest and highest",
                )
            tile(
                "Mean altitude",
                "–" if altitude is None else f"{altitude.mean:,.0f} m",
                delta=None if altitude is None else f"{altitude.min:,.0f}–{altitude.max:,.0f} m",
                delta_color="off",
                delta_arrow="off",
                help="Mean of the altitudes of points about 2.5 km apart in the hexagon of the place, "
                "from the Copernicus 90 m elevation model, with their lowest and highest; the sea counts as 0 m",
            )
        if weather is not None:
            # The grid point of the model of the day: the forecast comes from ECMWF IFS
            model = archive.ERA5.name if today is None else today["source"]
            model = archive.IFS.name if model == archive.FORECAST else model
            lat, lon, elevation = weather.points.get(model, (weather.latitude, weather.longitude, weather.elevation))
            st.caption(
                f"{day:%A %d %B %Y}, local time ({weather.timezone}). Values of the nearest {model} grid point, "
                f"{lat:.2f}° {lon:.2f}°, {elevation:.0f} m above sea level."
            )

    if weather is not None:
        month_tab, year_tab, day_tab = st.tabs(
            [
                f":material/calendar_month: Month · {day:%B %Y}",
                f":material/bar_chart: Year · {day:%Y}",
                f":material/schedule: Day · {day:%d %b %Y}",
            ]
        )
        on_day = alt.Tooltip("date:T", format="%a %d %b", title="Day")

        with month_tab:
            days_in_month = calendar.monthrange(day.year, day.month)[1]
            if month_days.empty:
                st.caption(f"No day of {day:%B %Y} is published completely yet.")
            else:
                if month is None:
                    st.caption(f"The statistics of {day:%B %Y} wait for its first day gone by.")
                else:
                    show_statistics(month)
                skies = [archive.sky(code) for code in month_days["weather_code"]]
                month_days = month_days.assign(
                    day=month_days["date"].dt.day,
                    direction=month_days["wind_direction_deg"].map(archive.compass),
                    icon=[icon for icon, _ in skies],
                    sky=[f"{icon} {label}" for icon, label in skies],
                )
                show_period_charts(
                    month_days,
                    "day",
                    list(range(1, days_in_month + 1)),
                    day.day,
                    "temperature_min_c",
                    "temperature_max_c",
                    [
                        on_day,
                        alt.Tooltip("sky:N", title="Sky"),
                        alt.Tooltip("temperature_max_c:Q", format=".1f", title="High °C"),
                        alt.Tooltip("temperature_mean_c:Q", format=".1f", title="Average °C"),
                        alt.Tooltip("temperature_min_c:Q", format=".1f", title="Low °C"),
                        alt.Tooltip("precipitation_mm:Q", format=".1f", title="Precipitation mm"),
                        alt.Tooltip("sunshine_h:Q", format=".1f", title="Sunshine h"),
                        alt.Tooltip("humidity_mean_pct:Q", format=".0f", title="Humidity %"),
                        alt.Tooltip("wind_speed_mean_kmh:Q", format=".0f", title="Wind km/h"),
                        alt.Tooltip("direction:N", title="Wind from"),
                        alt.Tooltip("wind_gusts_max_kmh:Q", format=".0f", title="Strongest gust km/h"),
                        alt.Tooltip("source:N", title="Source"),
                    ],
                    ["Daily weather and temperature, low to high", "Daily precipitation", "Daily mean wind",
                     "Daily sunshine"],
                )  # fmt: skip
                st.dataframe(
                    month_days[
                        [
                            "date",
                            "sky",
                            "temperature_max_c",
                            "temperature_mean_c",
                            "temperature_min_c",
                            "precipitation_mm",
                            "sunshine_h",
                            "humidity_mean_pct",
                            "cloud_cover_mean_pct",
                            "wind_speed_mean_kmh",
                            "direction",
                            "wind_gusts_max_kmh",
                            "source",
                        ]
                    ].rename(
                        columns={
                            "date": "Day",
                            "sky": "Sky",
                            "temperature_max_c": "High",
                            "temperature_mean_c": "Average",
                            "temperature_min_c": "Low",
                            "precipitation_mm": "Precipitation",
                            "sunshine_h": "Sunshine",
                            "humidity_mean_pct": "Humidity",
                            "cloud_cover_mean_pct": "Clouds",
                            "wind_speed_mean_kmh": "Wind",
                            "direction": "Wind from",
                            "wind_gusts_max_kmh": "Gusts",
                            "source": "Source",
                        }
                    ),
                    hide_index=True,
                    column_config={
                        "Day": st.column_config.DateColumn(format="ddd D MMM"),
                        "High": TEMPERATURE_COLUMN,
                        "Average": TEMPERATURE_COLUMN,
                        "Low": TEMPERATURE_COLUMN,
                        "Precipitation": PRECIPITATION_COLUMN,
                        "Sunshine": SUNSHINE_COLUMN,
                        "Humidity": PERCENT_COLUMN,
                        "Clouds": PERCENT_COLUMN,
                        "Wind": WIND_COLUMN,
                        "Gusts": WIND_COLUMN,
                    },
                )
                counts = month_days["source"].value_counts()
                st.caption(
                    " · ".join(f"{counts[source]} days of {source}" for source in archive.SOURCES if source in counts)
                    + (f" · {days_in_month - len(month_days)} days to come" if len(month_days) < days_in_month else "")
                    + ". The statistics leave the forecast out."
                )

        with year_tab:
            if past.empty:
                st.caption(f"No day of {day:%Y} is published completely yet.")
            else:
                show_statistics(archive.summary(past))
                months = archive.monthly(past)
                months["name"] = [MONTHS[first_day.month - 1] for first_day in months["month"]]
                show_period_charts(
                    months,
                    "name",
                    MONTHS,
                    MONTHS[day.month - 1],
                    "temperature_min_mean_c",
                    "temperature_max_mean_c",
                    [
                        alt.Tooltip("month:T", format="%B %Y", title="Month"),
                        alt.Tooltip("temperature_max_mean_c:Q", format=".1f", title="Average high °C"),
                        alt.Tooltip("temperature_mean_c:Q", format=".1f", title="Average °C"),
                        alt.Tooltip("temperature_min_mean_c:Q", format=".1f", title="Average low °C"),
                        alt.Tooltip("temperature_max_c:Q", format=".1f", title="Highest °C"),
                        alt.Tooltip("temperature_min_c:Q", format=".1f", title="Lowest °C"),
                        alt.Tooltip("precipitation_mm:Q", format=".0f", title="Precipitation mm"),
                        alt.Tooltip("wind_speed_mean_kmh:Q", format=".0f", title="Average wind km/h"),
                        alt.Tooltip("wind_gusts_max_kmh:Q", format=".0f", title="Strongest gust km/h"),
                        alt.Tooltip("sunshine_h:Q", format=".0f", title="Sunshine h"),
                        alt.Tooltip("humidity_mean_pct:Q", format=".0f", title="Average humidity %"),
                    ],
                    ["Monthly temperature, average low to average high", "Monthly precipitation", "Monthly mean wind",
                     "Monthly sunshine"],
                )  # fmt: skip
                st.dataframe(
                    months.drop(columns="month")[["name", *archive.STATISTICS]].rename(
                        columns={
                            "name": "Month",
                            "temperature_max_mean_c": "Average high",
                            "temperature_min_mean_c": "Average low",
                            "temperature_mean_c": "Average",
                            "temperature_max_c": "Highest",
                            "temperature_min_c": "Lowest",
                            "precipitation_mm": "Precipitation",
                            "wet_days": "Wet days",
                            "wind_speed_mean_kmh": "Average wind",
                            "wind_gusts_max_kmh": "Strongest gust",
                            "sunshine_h": "Sunshine",
                            "humidity_mean_pct": "Average humidity",
                            "days": "Days",
                        }
                    ),
                    hide_index=True,
                    column_config={
                        "Sunshine": st.column_config.NumberColumn(format="%.0f h"),
                        "Average humidity": PERCENT_COLUMN,
                        "Average high": TEMPERATURE_COLUMN,
                        "Average low": TEMPERATURE_COLUMN,
                        "Average": TEMPERATURE_COLUMN,
                        "Highest": TEMPERATURE_COLUMN,
                        "Lowest": TEMPERATURE_COLUMN,
                        "Precipitation": PRECIPITATION_COLUMN,
                        "Average wind": WIND_COLUMN,
                        "Strongest gust": WIND_COLUMN,
                    },
                )

        with day_tab:
            # Hours of the local clock, as text: a time given to the charts would be shown in the time zone
            # of the browser. On the day clocks go back, the repeated hour also names its time zone.
            # The icon of the weather of each hour, the moon at night
            skies = [
                archive.sky(code, by_day=is_day == 1)
                for code, is_day in zip(published["weather_code"], published["is_day"], strict=True)
            ]
            hours = published.assign(
                hour=published["time"].dt.strftime("%H"),
                label=published["time"].dt.strftime("%H:%M %Z"),
                icon=[icon for icon, _ in skies],
                sky=[f"{icon} {label}" for icon, label in skies],
                sunshine_min=published["sunshine_s"] / 60,
            )
            repeated = hours["hour"].duplicated(keep=False)
            hours.loc[repeated, "hour"] = hours.loc[repeated, "label"]
            plotted = hours[["hour", "temperature_c", "precipitation_mm", "icon", "sky"]]
            hour = alt.X("hour:O", sort=None, title=f"Hour ({weather.timezone})", axis=alt.Axis(labelAngle=0))
            # Today, a dashed rule at the current hour, the forecast on its right
            local_now = pd.Timestamp.now(tz=weather.timezone)
            now_hour = local_now.strftime("%H")
            now = alt.Chart(pd.DataFrame({"hour": [now_hour]})).mark_rule(strokeDash=[4, 4], opacity=0.6)
            marks_now = day == local_now.date() and now_hour in set(hours["hour"])

            def with_now(chart):
                return chart + now.encode(x="hour:O") if marks_now else chart

            charts = st.columns(2)
            temperature = (
                alt.Chart(plotted)
                .mark_line(strokeWidth=2, color=SERIES_COLOR[theme], point=alt.OverlayMarkDef(size=64))
                .encode(
                    x=hour,
                    # Room above the highest point for its icon
                    y=alt.Y("temperature_c:Q", title="°C", scale=alt.Scale(zero=False, padding=20)),
                    tooltip=[
                        alt.Tooltip("hour:O", title="Hour"),
                        alt.Tooltip("sky:N", title="Sky"),
                        alt.Tooltip("temperature_c:Q", format=".1f", title="°C"),
                    ],
                )
            )
            icons = alt.Chart(plotted).mark_text(dy=-15, fontSize=13).encode(x=hour, y="temperature_c:Q", text="icon:N")
            charts[0].altair_chart(
                styled(with_now(temperature + icons), 220, "Hourly weather and temperature"), width="stretch"
            )
            precipitation = (
                alt.Chart(plotted)
                .mark_bar(color=SERIES_COLOR[theme], cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=hour,
                    y=alt.Y("precipitation_mm:Q", title="mm", scale=from_zero(plotted["precipitation_mm"])),
                    tooltip=[
                        alt.Tooltip("hour:O", title="Hour"),
                        alt.Tooltip("precipitation_mm:Q", format=".2f", title="mm"),
                    ],
                )
            )
            charts[1].altair_chart(styled(with_now(precipitation), 200, "Hourly precipitation"), width="stretch")

            # The wind, solid, and the gusts, dashed, with arrows on the wind pointing where it blows: a ↓ for a
            # wind from the north, turned clockwise by its direction
            winds = hours[["hour", "wind_speed_kmh", "wind_gusts_kmh", "wind_direction_deg"]].assign(
                direction=hours["wind_direction_deg"].map(archive.compass)
            )
            series = winds.melt(
                id_vars="hour", value_vars=["wind_speed_kmh", "wind_gusts_kmh"], var_name="series", value_name="kmh"
            )
            series["series"] = series["series"].map({"wind_speed_kmh": "Wind", "wind_gusts_kmh": "Gusts"})
            names = ["Wind", "Gusts"]
            lines = (
                alt.Chart(series)
                .mark_line(strokeWidth=2)
                .encode(
                    x=hour,
                    y=alt.Y("kmh:Q", title="km/h"),
                    color=alt.Color(
                        "series:N",
                        scale=alt.Scale(domain=names, range=[SERIES_COLOR[theme], GUST_COLOR[theme]]),
                        legend=alt.Legend(title=None, orient="top", symbolType="stroke"),
                    ),
                    strokeDash=alt.StrokeDash(
                        "series:N", scale=alt.Scale(domain=names, range=[[1, 0], [5, 4]]), legend=None
                    ),
                )
            )
            arrows = (
                alt.Chart(winds)
                .mark_text(text="↓", fontSize=17, fontWeight="bold", color=SERIES_COLOR[theme])
                .encode(
                    x=hour,
                    y="wind_speed_kmh:Q",
                    angle=alt.Angle("wind_direction_deg:Q", scale=None),
                    tooltip=[
                        alt.Tooltip("hour:O", title="Hour"),
                        alt.Tooltip("wind_speed_kmh:Q", format=".0f", title="Wind km/h"),
                        alt.Tooltip("direction:N", title="From"),
                        alt.Tooltip("wind_gusts_kmh:Q", format=".0f", title="Gusts km/h"),
                    ],
                )
            )
            st.altair_chart(
                styled(with_now(lines + arrows), 220, "Hourly wind and gusts, the arrows pointing where it blows"),
                width="stretch",
            )

            # The humidity, solid, and the cloud cover, dashed, on the same 0-100 % scale
            moisture = hours[["hour", "humidity_pct", "cloud_cover_pct"]].melt(
                id_vars="hour", var_name="series", value_name="percent"
            )
            moisture["series"] = moisture["series"].map({"humidity_pct": "Humidity", "cloud_cover_pct": "Clouds"})
            names = ["Humidity", "Clouds"]
            humidity_clouds = (
                alt.Chart(moisture)
                .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=36))
                .encode(
                    x=hour,
                    y=alt.Y("percent:Q", title="%", scale=alt.Scale(domain=[0, 100])),
                    color=alt.Color(
                        "series:N",
                        scale=alt.Scale(domain=names, range=[SERIES_COLOR[theme], CLOUD_COLOR[theme]]),
                        legend=alt.Legend(title=None, orient="top", symbolType="stroke"),
                    ),
                    strokeDash=alt.StrokeDash(
                        "series:N", scale=alt.Scale(domain=names, range=[[1, 0], [5, 4]]), legend=None
                    ),
                    tooltip=[
                        alt.Tooltip("hour:O", title="Hour"),
                        alt.Tooltip("series:N", title=" "),
                        alt.Tooltip("percent:Q", format=".0f", title="%"),
                    ],
                )
            )
            st.altair_chart(styled(with_now(humidity_clouds), 200, "Hourly humidity and cloud cover"), width="stretch")
            if marks_now:
                st.caption(f"Now, {local_now:%H:%M}, is the dashed line: the hours after it are a forecast.")
            st.dataframe(
                hours.assign(direction=winds["direction"])[
                    [
                        "label",
                        "sky",
                        "temperature_c",
                        "precipitation_mm",
                        "humidity_pct",
                        "cloud_cover_pct",
                        "sunshine_min",
                        "wind_speed_kmh",
                        "direction",
                        "wind_gusts_kmh",
                        "source",
                    ]
                ].rename(
                    columns={
                        "label": "Hour",
                        "sky": "Sky",
                        "temperature_c": "Temperature",
                        "precipitation_mm": "Precipitation",
                        "humidity_pct": "Humidity",
                        "cloud_cover_pct": "Clouds",
                        "sunshine_min": "Sunshine",
                        "wind_speed_kmh": "Wind",
                        "direction": "Wind from",
                        "wind_gusts_kmh": "Gusts",
                        "source": "Source",
                    }
                ),
                hide_index=True,
                column_config={
                    "Temperature": TEMPERATURE_COLUMN,
                    "Precipitation": st.column_config.NumberColumn(format="%.2f mm"),
                    "Humidity": PERCENT_COLUMN,
                    "Clouds": PERCENT_COLUMN,
                    "Sunshine": st.column_config.NumberColumn(format="%.0f min"),
                    "Wind": WIND_COLUMN,
                    "Gusts": WIND_COLUMN,
                },
            )
            if selected.empty and not published.empty:
                st.caption(f"Published so far: the hours up to {published['time'].max():%H:%M %Z}.")

        st.caption(
            "Weather of the place: ERA5 reanalysis of the Copernicus Climate Change Service, then the ECMWF IFS "
            "model for the days ERA5 has not published yet and the forecast, served by "
            "[Open-Meteo](https://open-meteo.com) under CC BY 4.0. Altitude: Copernicus DEM GLO-90."
        )

if dates:
    st.subheader("Global mean temperature")
    series = load(warehouse, "global_daily_temperature")
    line = (
        alt.Chart(series)
        .mark_line(strokeWidth=2, color=SERIES_COLOR[theme])
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("temperature_mean_c:Q", title="°C", scale=alt.Scale(zero=False)),
            tooltip=[
                alt.Tooltip("date:T", format="%d %b %Y"),
                alt.Tooltip("temperature_mean_c:Q", format=".2f", title="°C"),
            ],
        )
    )
    selected_day = alt.Chart(series[series["date"] == day]).mark_rule(strokeDash=[4, 4], opacity=0.5)
    st.altair_chart(
        styled(line + selected_day.encode(x="date:T"), 220, "Area-weighted mean of the ingested days"),
        width="stretch",
    )

if on_map:
    with st.expander("Data of the map"):
        st.dataframe(values.drop(columns=["color"]), hide_index=True)
