"""Queries of the dashboard on the dbt marts, and the color scales of the map layers."""

import os
from dataclasses import dataclass
from datetime import date

import duckdb
import h3
import numpy as np
import pandas as pd

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_WAREHOUSE = os.path.join(PROJECT_ROOT, "output", "warehouse.duckdb")

# Resolution of the hexagons of a zoomed-in map. The 0.25° ERA5 grid is sparser than the resolution 5
# cells (about half of them hold no grid point at mid latitudes): every resolution 4 cell holds some.
ZOOM_RESOLUTION = 4
# Half height of the map window around a searched place, in degrees of latitude (twice as wide in longitude)
PLACE_WINDOW_DEG = 8.0
# Invisible hexagons make the map clickable: about 360 km across on the world, where a first click zooms in,
# 140 km on an area, and by default 20 km around a place, finer than the 0.25° (28 km) grid of ERA5
PICK_RESOLUTION = {"world": 2, "area": 3, "place": 5}
# The sizes of the hexagons around a place to choose from, from 140 to 3 km across, and the one of a place by
# default: the finest, 2.8 km across and about 5 km², whose altitude is the closest to the place's own
PLACE_RESOLUTIONS = (3, 4, 5, 6, 7)
PLACE_RESOLUTION = 7
# Half height of the clickable window around a place with resolution 5 hexagons, about 4,500 of them:
# the map shows about 6° of latitude at its zoom. The window scales with the hexagons, keeping their number.
PICK_WINDOW_DEG = 4.0

# Diverging blue <-> red around a neutral gray midpoint (light and dark variants), equal steps per arm
COLD_ARM = ["#104281", "#256abf", "#5598e7", "#9ec5f4"]
WARM_ARM = ["#f4a6a0", "#ec7a71", "#e34948", "#a32a2a"]
NEUTRAL = {"light": "#f0efec", "dark": "#383835"}
# Sequential, one hue light -> dark
BLUES = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
ORANGES = ["#fbd9c9", "#f6b08f", "#f08a5f", "#eb6834", "#c4501f", "#9a3c14", "#6f2a0c"]


@dataclass(frozen=True)
class Layer:
    """A map layer: what is drawn and how its values map to colors."""

    label: str
    unit: str
    scale: str  # "diverging" around center, or "sequential" from low
    low: float
    high: float
    center: float = 0.0
    regional: bool = True  # available on the resolution 3 regions
    events: bool = False  # sparse cells of extreme events, drawn at resolution 5 worldwide


LAYERS = {
    "temperature_mean_c": Layer("Mean temperature", "°C", "diverging", -40, 40),
    "temperature_max_c": Layer("Maximum temperature", "°C", "diverging", -40, 45),
    "precipitation_mm": Layer("Precipitation", "mm", "sequential", 0, 50),
    "temperature_anomaly_c": Layer("Temperature anomaly", "°C", "diverging", -10, 10),
    "heatwave_excess_c": Layer("Heatwaves", "°C above the hot day threshold", "sequential", 0, 8, events=True),
    "extreme_precipitation_ratio": Layer(
        "Extreme precipitation", "× the 95th percentile", "sequential", 1, 4, events=True
    ),
}

REGIONS = {
    "Europe": (34.0, 72.0, -25.0, 45.0),
    "Italy": (35.5, 47.5, 6.0, 19.0),
    "Alps": (43.5, 48.5, 5.0, 17.0),
    "North America": (15.0, 72.0, -170.0, -50.0),
    "South Asia": (5.0, 37.0, 60.0, 100.0),
    "Australia": (-45.0, -9.0, 110.0, 157.0),
}


def connect(path: str = DEFAULT_WAREHOUSE) -> duckdb.DuckDBPyConnection:
    """Read-only connection: the dashboard never locks the warehouse against dbt for long."""
    return duckdb.connect(path, read_only=True)


def place_window(
    latitude: float, longitude: float, half_height: float = PLACE_WINDOW_DEG
) -> tuple[float, float, float, float]:
    """Bounding box (lat_min, lat_max, lon_min, lon_max) of the map around a place, twice as wide as high."""
    return (
        max(latitude - half_height, -90.0),
        min(latitude + half_height, 90.0),
        longitude - 2 * half_height,
        longitude + 2 * half_height,
    )


def pick_window(latitude: float, longitude: float, resolution: int) -> tuple[float, float, float, float]:
    """The window of the clickable hexagons of a resolution around a place: each resolution is √7 times finer."""
    return place_window(latitude, longitude, PICK_WINDOW_DEG * 7 ** ((PICK_RESOLUTION["place"] - resolution) / 2))


def hexagon_size(resolution: int) -> str:
    """The size of the hexagons of a resolution, from corner to corner."""
    across = 2 * h3.average_hexagon_edge_length(resolution, unit="km")
    return f"{across:.0f} km" if across >= 10 else f"{across:.1f} km"


def pick_cells(bbox: tuple[float, float, float, float] | None, resolution: int) -> pd.DataFrame:
    """
    Hexagons covering a bounding box, or the world, with the coordinates of their center as tooltip:
    drawn invisible under the map, they make every point of it clickable.
    """
    if bbox is None:
        cells = [child for cell in h3.get_res0_cells() for child in h3.cell_to_children(cell, resolution)]
    else:
        lat_min, lat_max, lon_min, lon_max = bbox
        lon_min, lon_max = max(lon_min, -180.0), min(lon_max, 180.0)
        outline = [(lat_min, lon_min), (lat_min, lon_max), (lat_max, lon_max), (lat_max, lon_min)]
        cells = list(h3.polygon_to_cells(h3.LatLngPoly(outline), resolution))
    centers = [h3.cell_to_latlng(cell) for cell in cells]
    tooltips = [f"{lat:.2f}°, {lon:.2f}° · click for its weather" for lat, lon in centers]
    return pd.DataFrame({"h3": cells, "value_text": tooltips})


def available_dates(con: duckdb.DuckDBPyConnection) -> list[date]:
    rows = con.execute("select distinct date from marts.agg_daily_region_weather order by date").fetchall()
    return [row[0] for row in rows]


def global_daily_temperature(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Area-weighted mean temperature of the processed grid, per day."""
    return con.execute(
        """
        with region_area as (
            select region_h3_index, sum(area_km2) as area_km2 from marts.dim_h3_cell group by region_h3_index
        )
        select date, sum(temperature_mean_c * area_km2) / sum(area_km2) as temperature_mean_c
        from marts.agg_daily_region_weather
        inner join region_area using (region_h3_index)
        where temperature_mean_c is not null
        group by date
        order by date
        """
    ).df()


def headline(con: duckdb.DuckDBPyConnection, day: date) -> dict[str, float | int | None]:
    """The numbers of the KPI row for a day."""
    temperature = con.execute(
        """
        select
            sum(w.temperature_mean_c * c.area_km2) / sum(c.area_km2) as global_mean_c,
            max(w.temperature_max_c) as hottest_c,
            min(w.temperature_min_c) as coldest_c,
            sum(w.precipitation_mm * c.area_km2) / sum(c.area_km2) as global_precipitation_mm
        from marts.fct_daily_cell_weather as w
        inner join marts.dim_h3_cell as c using (h3_index)
        where w.date = ?
        """,
        [day],
    ).fetchone()
    heatwave_cells = con.execute(
        "select count(*) from marts.fct_heatwave_events where ? between start_date and end_date", [day]
    ).fetchone()
    extreme_cells = con.execute(
        "select count(*) from marts.fct_extreme_precipitation_days where date = ?", [day]
    ).fetchone()
    keys = ("global_mean_c", "hottest_c", "coldest_c", "global_precipitation_mm")
    return {
        **dict(zip(keys, temperature or (None,) * 4, strict=True)),
        "heatwave_cells": heatwave_cells[0] if heatwave_cells else 0,
        "extreme_precipitation_cells": extreme_cells[0] if extreme_cells else 0,
    }


def layer_values(
    con: duckdb.DuckDBPyConnection,
    day: date,
    layer_key: str,
    bbox: tuple[float, float, float, float] | None = None,
    resolution: int = ZOOM_RESOLUTION,
) -> pd.DataFrame:
    """
    Values of a layer for a day, one row per hexagon (h3, value).

    Without a bounding box, continuous layers are drawn on the resolution 3 regions; with one, on the cells
    of the resolution inside it, at most ZOOM_RESOLUTION. Event layers are sparse: always their resolution 5 cells.
    """
    cell_filter = ""
    params: list[object] = [day]
    if bbox is not None:
        lat_min, lat_max, lon_min, lon_max = bbox
        cell_filter = "and c.center_latitude between ? and ? and c.center_longitude between ? and ?"
        params += [lat_min, lat_max, lon_min, lon_max]

    if layer_key == "heatwave_excess_c":
        sql = f"""
            select h.h3_index as h3, h.mean_excess_c as value, h.start_date, h.end_date
            from marts.fct_heatwave_events as h
            inner join marts.dim_h3_cell as c using (h3_index)
            where ? between h.start_date and h.end_date {cell_filter}
        """
    elif layer_key == "extreme_precipitation_ratio":
        sql = f"""
            select e.h3_index as h3, e.ratio_to_threshold as value, e.precipitation_mm
            from marts.fct_extreme_precipitation_days as e
            inner join marts.dim_h3_cell as c using (h3_index)
            where e.date = ? {cell_filter}
        """
    elif layer_key == "temperature_anomaly_c":
        sql = f"""
            select a.h3_index as h3, a.temperature_anomaly_c as value
            from marts.fct_daily_temperature_anomaly as a
            inner join marts.dim_h3_cell as c using (h3_index)
            where a.date = ? {cell_filter}
        """
        if bbox is None:
            sql = """
                select c.region_h3_index as h3, avg(a.temperature_anomaly_c) as value
                from marts.fct_daily_temperature_anomaly as a
                inner join marts.dim_h3_cell as c using (h3_index)
                where a.date = ?
                group by c.region_h3_index
            """
    elif bbox is None:
        sql = f"""
            select region_h3_index as h3, {layer_key} as value, cells
            from marts.agg_daily_region_weather
            where date = ?
        """
    else:
        sql = f"""
            select w.h3_index as h3, w.{layer_key} as value
            from marts.fct_daily_cell_weather as w
            inner join marts.dim_h3_cell as c using (h3_index)
            where w.date = ? {cell_filter}
        """

    df = con.execute(sql, params).df()
    df = df[df["value"].notna()]
    if bbox is not None and not LAYERS[layer_key].events:
        # The children of a cell have nearly the same area: their plain mean is the value of the parent
        parent = min(resolution, ZOOM_RESOLUTION)
        df = df.assign(h3=[h3.cell_to_parent(cell, parent) for cell in df["h3"]])
        df = df.groupby("h3", as_index=False).agg(value=("value", "mean"))
    return df.reset_index(drop=True)


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    return tuple(int(color[i : i + 2], 16) for i in (1, 3, 5))  # type: ignore[return-value]


def scale_colors(layer: Layer, theme: str = "light") -> list[str]:
    """Color stops of the layer, from low to high."""
    if layer.scale == "diverging":
        return [*COLD_ARM, NEUTRAL[theme], *WARM_ARM]
    return ORANGES if layer.events and layer.unit.startswith("°C") else BLUES


def colorize(values: pd.Series, layer: Layer, theme: str = "light", alpha: int = 220) -> list[list[int]]:
    """Map values to RGBA colors, interpolating the stops of the layer; values beyond the scale are clipped."""
    stops = np.array([hex_to_rgb(c) for c in scale_colors(layer, theme)], dtype=float)
    x = values.to_numpy(dtype=float)
    if layer.scale == "diverging":
        # The neutral midpoint sits exactly on the center value, each arm spans its half of the range
        position = np.where(
            x < layer.center,
            0.5 * (x - layer.low) / (layer.center - layer.low),
            0.5 + 0.5 * (x - layer.center) / (layer.high - layer.center),
        )
    else:
        position = (x - layer.low) / (layer.high - layer.low)
    position = np.clip(position, 0, 1) * (len(stops) - 1)
    lower = np.floor(position).astype(int)
    upper = np.minimum(lower + 1, len(stops) - 1)
    fraction = (position - lower)[:, None]
    rgb = stops[lower] * (1 - fraction) + stops[upper] * fraction
    return [[*map(int, color), alpha] for color in np.rint(rgb)]
