import os

import h3
import pytest
from conftest import make_precipitation_df, make_temperature_df

from utils.data_writer import daily_output_path, save_data_to_parquet
from utils.h3_processor import map_h3_indices_to_dataframe, precompute_h3_index_lookup
from utils.query_runner import build_filter_expression, run_queries_on_data

DAYS = ["2022-03-31", "2022-04-01", "2022-04-30", "2022-05-01"]


def write_dataset(output, variable_name, make_df):
    for day in DAYS:
        df = make_df([day])
        df = map_h3_indices_to_dataframe(df, precompute_h3_index_lookup(df, 5))
        ts = df["time"].iloc[0]
        save_data_to_parquet(df, str(output), variable_name, ts.year, ts.month, ts.day)


@pytest.fixture
def output_dir(tmp_path):
    output = tmp_path / "output"
    write_dataset(output, "total_precipitation", make_precipitation_df)
    write_dataset(output, "2m_temperature", make_temperature_df)
    return str(output)


def run(output_dir, tmp_path, *queries):
    return run_queries_on_data(output_dir, str(tmp_path / "q"), list(queries))


def test_files_are_partitioned_by_variable_year_and_month(output_dir):
    path = daily_output_path(output_dir, "2m_temperature", 2022, 4, 30)
    assert os.path.exists(path)
    assert os.path.join("2m_temperature", "year=2022", "month=04", "2022-04-30_2m_temperature.parquet") in path
    assert not any(name.endswith(".tmp") for _, _, files in os.walk(output_dir) for name in files)


def test_end_day_is_fully_included(output_dir, tmp_path):
    query = {"name": "april", "variable": "total_precipitation", "start": "2022-04-01", "end": "2022-04-30"}
    df = run(output_dir, tmp_path, query)["april"]

    assert df["time"].min().isoformat() == "2022-04-01T00:00:00"
    assert df["time"].max().isoformat() == "2022-04-30T23:00:00"
    assert len(df) == 2 * 24 * 4
    assert set(df["year"]) == {2022} and set(df["month"]) == {4}
    assert "t2m" not in df.columns


def test_precipitation_point_sum_in_mm(output_dir, tmp_path):
    query = {
        "name": "zurich",
        "variable": "total_precipitation",
        "point": {"latitude": 47.25, "longitude": 8.5, "tolerance": 0.01},
        "start": "2022-04-01",
        "end": "2022-04-30",
        "aggregate": "sum",
    }
    result = run(output_dir, tmp_path, query)["zurich"]
    # 2 days x 24 hours x 1 grid point x 0.001 m
    assert result == {"sum": pytest.approx(48.0, rel=1e-4)}
    assert not os.path.exists(tmp_path / "q" / "zurich.parquet")


def test_temperature_statistics_in_celsius(output_dir, tmp_path):
    query = {
        "name": "zurich_t",
        "variable": "2m_temperature",
        "point": {"latitude": 47.25, "longitude": 8.5},
        "start": "2022-04-01",
        "end": "2022-04-30",
        "aggregate": ["mean", "min", "max"],
    }
    result = run(output_dir, tmp_path, query)["zurich_t"]
    # 10 °C base, -5 °C at midnight, +6.5 °C at 23:00
    assert result["min"] == pytest.approx(5.0, abs=1e-3)
    assert result["max"] == pytest.approx(16.5, abs=1e-3)
    assert result["mean"] == pytest.approx(10.75, abs=1e-3)


def test_temperature_rows_keep_kelvin(output_dir, tmp_path):
    query = {"name": "rows", "variable": "2m_temperature", "start": "2022-05-01", "end": "2022-05-01"}
    df = run(output_dir, tmp_path, query)["rows"]
    assert len(df) == 24 * 4
    assert df["t2m"].min() > 273
    assert os.path.exists(tmp_path / "q" / "rows.parquet")


def test_h3_and_bbox_filters(output_dir, tmp_path):
    cell = h3.latlng_to_cell(47.5, 8.75, 5)
    results = run(
        output_dir,
        tmp_path,
        {"name": "cell", "variable": "total_precipitation", "h3_index": cell},
        {
            "name": "box",
            "variable": "2m_temperature",
            "bbox": {"lat_min": 47.4, "lat_max": 47.6, "lon_min": 8.6, "lon_max": 8.8},
        },
    )

    assert not results["cell"].empty and (results["cell"]["h3_index"] == cell).all()
    assert set(zip(results["box"]["latitude"], results["box"]["longitude"], strict=True)) == {(47.5, 8.75)}


def test_sum_of_temperature_is_rejected(output_dir, tmp_path):
    with pytest.raises(ValueError, match=r"sum.*2m_temperature"):
        run(output_dir, tmp_path, {"name": "x", "variable": "2m_temperature", "aggregate": "sum"})


def test_query_without_variable_is_rejected(output_dir, tmp_path):
    with pytest.raises(ValueError, match="variable"):
        run(output_dir, tmp_path, {"name": "x"})


def test_query_on_unprocessed_variable_is_skipped(tmp_path):
    output = tmp_path / "output"
    write_dataset(output, "total_precipitation", make_precipitation_df)
    results = run(
        str(output),
        tmp_path,
        {"name": "t", "variable": "2m_temperature", "aggregate": "mean"},
        {"name": "p", "variable": "total_precipitation", "aggregate": "sum"},
    )
    assert set(results) == {"p"}


def test_unknown_query_keys_raise():
    with pytest.raises(ValueError, match="h3"):
        build_filter_expression({"name": "typo", "variable": "total_precipitation", "h3": "85032097fffffff"})
