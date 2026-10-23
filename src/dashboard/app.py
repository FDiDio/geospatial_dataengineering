"""
Map of the daily ERA5 weather on H3 hexagons, from the dbt marts.

    uv run streamlit run src/dashboard/app.py

The warehouse is DUCKDB_PATH, by default output/warehouse.duckdb.
"""

import os
import sys

import altair as alt
import duckdb
import pydeck as pdk
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import queries
from dashboard.queries import DEFAULT_WAREHOUSE, LAYERS, REGIONS, colorize, scale_colors

WORLD = "World (H3 regions, ~12,000 km²)"
SERIES_COLOR = {"light": "#2a78d6", "dark": "#3987e5"}

st.set_page_config(page_title="ERA5 weather map", page_icon="🌍", layout="wide")
theme = getattr(getattr(st.context, "theme", None), "type", None) or "light"
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


try:
    dates = load(warehouse, "available_dates")
except duckdb.Error as error:
    st.title("ERA5 weather map")
    st.info(
        f"No marts found in `{warehouse}` ({error}). Ingest some days and build the dbt models first:\n\n"
        "```\nuv run python src/main_script.py --start_date 20220701 --end_date 20220731\n"
        "cd transform && uv run dbt build\n```"
    )
    st.stop()

if not dates:
    st.warning("The warehouse has no processed day yet.")
    st.stop()

with st.sidebar:
    st.header("ERA5 weather map")
    day = st.select_slider("Day", options=dates, value=dates[-1], format_func=lambda d: d.strftime("%d %b %Y"))
    layer_key = st.radio("Layer", list(LAYERS), format_func=lambda key: LAYERS[key].label)
    layer = LAYERS[layer_key]
    area = st.selectbox("Area", [WORLD, *REGIONS], help="Regions are rolled up from the resolution 5 cells")
    bbox = REGIONS.get(area)

kpis = load(warehouse, "headline", day)
columns = st.columns(5)
columns[0].metric("Global mean temperature", f"{kpis['global_mean_c']:.1f} °C" if kpis["global_mean_c"] else "–")
columns[1].metric("Hottest cell", f"{kpis['hottest_c']:.1f} °C" if kpis["hottest_c"] is not None else "–")
columns[2].metric("Coldest cell", f"{kpis['coldest_c']:.1f} °C" if kpis["coldest_c"] is not None else "–")
columns[3].metric("Cells in a heatwave", f"{kpis['heatwave_cells']:,}")
columns[4].metric("Extreme precipitation cells", f"{kpis['extreme_precipitation_cells']:,}")

values = load(warehouse, "layer_values", day, layer_key, bbox).copy()
values["color"] = colorize(values["value"], layer, theme)
values["value_text"] = values["value"].map(lambda v: f"{v:,.1f} {layer.unit}")

if bbox is None:
    view = pdk.ViewState(latitude=25, longitude=10, zoom=1)
else:
    lat_min, lat_max, lon_min, lon_max = bbox
    view = pdk.ViewState(latitude=(lat_min + lat_max) / 2, longitude=(lon_min + lon_max) / 2, zoom=3.5)

st.pydeck_chart(
    pdk.Deck(
        layers=[
            pdk.Layer(
                "H3HexagonLayer",
                values[["h3", "value_text", "color"]],
                get_hexagon="h3",
                get_fill_color="color",
                extruded=False,
                stroked=False,
                pickable=True,
                auto_highlight=True,
            )
        ],
        initial_view_state=view,
        map_provider="carto",
        map_style="dark" if theme == "dark" else "light",
        tooltip={"html": f"<b>{layer.label}</b><br/>{{value_text}}<br/><small>{{h3}}</small>"},
    ),
    height=560,
)

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
selected = alt.Chart(series[series["date"] == day]).mark_rule(strokeDash=[4, 4], opacity=0.5).encode(x="date:T")
st.altair_chart((line + selected).properties(height=220), width="stretch")

with st.expander("Data of the map"):
    st.dataframe(values.drop(columns=["color"]), hide_index=True)
