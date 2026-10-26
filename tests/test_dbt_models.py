"""End-to-end tests of the dbt models on a synthetic scenario ingested by the real pipeline."""

import os
from datetime import date, timedelta

import duckdb
import h3
import numpy as np
import pandas as pd
import pytest
from conftest import fake_archive_fetch, make_era5_df, to_data_array
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.cli.main import dbtRunner
from streamlit.testing.v1 import AppTest

from dashboard import archive, places, queries, terrain
from utils.data_writer import ParquetSink
from utils.h3_processor import H3Enricher
from utils.iceberg_sink import IcebergSink, local_catalog_properties
from utils.pipeline import Pipeline
from utils.variables import get_variable

pytestmark = pytest.mark.integration

PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "transform")
DAYS = [date(2022, 4, 1) + timedelta(days=n) for n in range(61)]  # April and May 2022
HEATWAVE = {date(2022, 4, 10), date(2022, 4, 11), date(2022, 4, 12)}
ISOLATED_HOT_DAYS = {date(2022, 5, 5), date(2022, 5, 20)}
EXTREME_RAIN_DAY = date(2022, 4, 25)
GRID_POINTS = [(lat, lon) for lat in (47.25, 47.5) for lon in (8.5, 8.75)]
CELLS = {h3.latlng_to_cell(lat, lon, 5) for lat, lon in GRID_POINTS}


def is_wet(day):
    return day.toordinal() % 3 == 0 or day == EXTREME_RAIN_DAY


class ScenarioSource:
    """
    Two months of synthetic weather on 4 grid points: a 3-day heatwave in April, two isolated
    hot days in May, a wet day out of three (2.4 mm) and one extreme wet day (48 mm).
    """

    def read(self, field, year, month, day):
        current = date(year, month, day)
        if field.name == "2m_temperature":
            df = make_era5_df([current.isoformat()], "t2m", 288.15)  # 15 °C
            hot = current in HEATWAVE or current in ISOLATED_HOT_DAYS
            df["t2m"] = (df["t2m"] + df["time"].dt.hour * 0.1 + (15 if hot else 0)).astype("float32")
        else:
            hourly = 0.002 if current == EXTREME_RAIN_DAY else 0.0001 if is_wet(current) else 0.0
            df = make_era5_df([current.isoformat()], "tp", hourly)
        return to_data_array(df, field.short_name)


def ingest(sink, lake):
    pipeline = Pipeline(ScenarioSource(), sink, H3Enricher(5, os.path.join(lake, "h3_lookup")))
    for variable in ("total_precipitation", "2m_temperature"):
        for day in DAYS:
            pipeline.process(get_variable(variable), day.year, day.month, day.day)


def dbt(args, env, monkeypatch, tmp_path):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    result = dbtRunner().invoke(
        [*args, "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR,
         "--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs")]
    )  # fmt: skip
    # dbt-duckdb keeps its connection open in the process: release the database file
    DuckDBConnectionManager.close_all_connections()
    failures = [(r.node.name, str(r.status), r.message) for r in getattr(result.result, "results", [])
                if str(r.status) not in ("success", "pass")]  # fmt: skip
    assert result.success, result.exception or failures
    return result


def query(env, sql):
    with duckdb.connect(env["DUCKDB_PATH"], read_only=True) as con:
        con.execute("LOAD h3")
        return con.execute(sql).df()


@pytest.fixture(scope="module")
def parquet_env(tmp_path_factory):
    lake = str(tmp_path_factory.mktemp("lake"))
    ingest(ParquetSink(lake), lake)
    return {"ERA5_LAKE_PATH": lake, "DUCKDB_PATH": os.path.join(lake, "warehouse.duckdb"), "ERA5_FORMAT": "parquet"}


@pytest.fixture(scope="module")
def built(parquet_env, tmp_path_factory):
    with pytest.MonkeyPatch.context() as monkeypatch:
        dbt(["build"], parquet_env, monkeypatch, tmp_path_factory.mktemp("dbt"))
    return parquet_env


def test_daily_cell_weather(built):
    df = query(built, "select * from marts.fct_daily_cell_weather")

    assert len(df) == len(DAYS) * len(CELLS)
    assert set(df["h3_index"]) == CELLS
    normal = df[pd.to_datetime(df["date"]).dt.date == date(2022, 4, 2)].iloc[0]
    assert normal["temperature_min_c"] == pytest.approx(15.0, abs=1e-3)
    assert normal["temperature_max_c"] == pytest.approx(17.3, abs=1e-3)
    wet = df[pd.to_datetime(df["date"]).dt.date == EXTREME_RAIN_DAY]
    assert wet["precipitation_mm"].tolist() == pytest.approx([48.0] * len(CELLS), rel=1e-4)


def test_heatwave_is_detected_and_isolated_hot_days_are_not(built):
    events = query(built, "select * from marts.fct_heatwave_events")

    assert len(events) == len(CELLS)
    assert set(pd.to_datetime(events["start_date"]).dt.date) == {min(HEATWAVE)}
    assert set(pd.to_datetime(events["end_date"]).dt.date) == {max(HEATWAVE)}
    assert (events["duration_days"] == 3).all()
    assert events["peak_temperature_max_c"].tolist() == pytest.approx([32.3] * len(CELLS), abs=1e-3)


def test_extreme_precipitation_day(built):
    extremes = query(built, "select * from marts.fct_extreme_precipitation_days")

    assert set(pd.to_datetime(extremes["date"]).dt.date) == {EXTREME_RAIN_DAY}
    assert extremes["extreme_threshold_mm"].tolist() == pytest.approx([2.4] * len(CELLS), rel=1e-3)


def test_temperature_anomaly_is_positive_on_hot_days(built):
    df = query(built, "select * from marts.fct_daily_temperature_anomaly")
    hot = pd.to_datetime(df["date"]).dt.date.isin(HEATWAVE | ISOLATED_HOT_DAYS)

    assert (df.loc[hot, "temperature_anomaly_c"] > 10).all()
    assert (df.loc[~hot, "temperature_anomaly_c"] < 0).all()


def test_regional_rollup(built):
    regions = query(built, "select * from marts.agg_daily_region_weather")
    cells = query(built, "select * from marts.dim_h3_cell")

    assert set(regions["region_h3_index"]) == {h3.cell_to_parent(cell, 3) for cell in CELLS}
    assert (regions.groupby("date")["cells"].sum() == len(CELLS)).all()
    assert set(cells["h3_index"]) == CELLS
    assert cells["area_km2"].between(200, 300).all()


def test_dashboard_layers(built):
    with queries.connect(built["DUCKDB_PATH"]) as con:
        assert queries.available_dates(con) == DAYS
        heatwave_day = min(HEATWAVE)
        assert set(queries.layer_values(con, heatwave_day, "heatwave_excess_c")["h3"]) == CELLS
        assert queries.layer_values(con, heatwave_day + timedelta(days=5), "heatwave_excess_c").empty
        assert set(queries.layer_values(con, EXTREME_RAIN_DAY, "extreme_precipitation_ratio")["h3"]) == CELLS
        regions = queries.layer_values(con, heatwave_day, "temperature_mean_c")
        assert set(regions["h3"]) == {h3.cell_to_parent(cell, 3) for cell in CELLS}
        cells = queries.layer_values(con, heatwave_day, "temperature_anomaly_c", queries.REGIONS["Alps"])
        assert set(cells["h3"]) == {h3.cell_to_parent(cell, queries.ZOOM_RESOLUTION) for cell in CELLS}
        # Larger hexagons chosen around a place: the cells are rolled up to them, finer ones stop at ZOOM_RESOLUTION
        coarse = queries.layer_values(con, heatwave_day, "temperature_anomaly_c", queries.REGIONS["Alps"], 3)
        assert set(coarse["h3"]) == {h3.cell_to_parent(cell, 3) for cell in CELLS}
        fine = queries.layer_values(con, heatwave_day, "temperature_anomaly_c", queries.REGIONS["Alps"], 7)
        assert set(fine["h3"]) == set(cells["h3"])
        assert (cells["value"] > 10).all()
        kpis = queries.headline(con, heatwave_day)
        assert kpis["heatwave_cells"] == len(CELLS)
        assert kpis["hottest_c"] == pytest.approx(32.3, abs=1e-3)


def test_dashboard_renders(built, monkeypatch):
    monkeypatch.setenv("DUCKDB_PATH", built["DUCKDB_PATH"])
    monkeypatch.setattr(places, "search", lambda query, language: [places.Place("Zürich, Schweiz", 47.37, 8.54)])
    monkeypatch.setattr(archive, "fetch", fake_archive_fetch)
    monkeypatch.setattr(terrain, "cell_altitude", lambda cell: terrain.Altitude(412.0, 390.0, 870.0, 49))
    app = AppTest.from_file(os.path.join(os.path.dirname(__file__), "..", "src", "dashboard", "app.py"))
    app.run(timeout=60)

    assert not app.exception
    assert [metric.label for metric in app.metric][:2] == ["Global mean temperature", "Hottest cell"]
    app.sidebar.radio[0].set_value("heatwave_excess_c").run(timeout=60)
    assert not app.exception
    app.sidebar.text_input[0].set_value("Zurich").run(timeout=60)
    assert not app.exception
    assert app.subheader[0].value == "Zürich"
    assert [tab.label.split(": ", 1)[-1] for tab in app.tabs] == [
        "Month · May 2022",
        "Year · 2022",
        "Day · 31 May 2022",
    ]
    assert len(app.dataframe) == 4  # the days of the month, the months of the year, the hours and the data of the map
    # A day out of the warehouse: the weather of the place, no map
    app.sidebar.date_input[0].set_value(date(2022, 7, 15)).run(timeout=60)
    assert not app.exception
    assert any("ingested days only" in caption.value for caption in app.caption)
    assert [tab.label.split(": ", 1)[-1] for tab in app.tabs] == [
        "Month · July 2022",
        "Year · 2022",
        "Day · 15 Jul 2022",
    ]


def test_incremental_run_rebuilds_only_the_selected_days(built, monkeypatch, tmp_path):
    lake = built["ERA5_LAKE_PATH"]
    # Late correction of a day: 10 °C warmer
    pipeline = Pipeline(ScenarioSource(), ParquetSink(lake), H3Enricher(5, os.path.join(lake, "h3_lookup")))
    original_read = pipeline.source.read
    pipeline.source.read = lambda field, *d: original_read(field, *d) + 10
    pipeline.process(get_variable("2m_temperature"), 2022, 5, 30)

    before = query(built, "select * from marts.fct_daily_cell_weather order by date, h3_index")
    dbt(["run", "--vars", "{start_date: 2022-05-30}"], built, monkeypatch, tmp_path)
    after = query(built, "select * from marts.fct_daily_cell_weather order by date, h3_index")

    changed = pd.to_datetime(after["date"]).dt.date == date(2022, 5, 30)
    assert len(after) == len(before)
    assert np.allclose(after.loc[changed, "temperature_mean_c"], before.loc[changed, "temperature_mean_c"] + 10)
    pd.testing.assert_frame_equal(after[~changed], before[~changed])


def test_iceberg_tables_give_the_same_marts(tmp_path, monkeypatch):
    lake = str(tmp_path / "lake")
    warehouse = os.path.join(lake, "iceberg")
    ingest(IcebergSink(local_catalog_properties(warehouse)), lake)
    env = {
        "ERA5_FORMAT": "iceberg",
        "ERA5_ICEBERG_WAREHOUSE": warehouse,
        "ERA5_LAKE_PATH": lake,
        "DUCKDB_PATH": os.path.join(lake, "warehouse.duckdb"),
    }
    dbt(["build"], env, monkeypatch, tmp_path)

    events = query(env, "select * from marts.fct_heatwave_events")
    assert len(events) == len(CELLS) and (events["duration_days"] == 3).all()
    extremes = query(env, "select * from marts.fct_extreme_precipitation_days")
    assert set(pd.to_datetime(extremes["date"]).dt.date) == {EXTREME_RAIN_DAY}
