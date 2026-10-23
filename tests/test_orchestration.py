import importlib
import os

import dagster as dg
import pytest
import yaml
from conftest import FakeSource
from dagster_dbt import DbtCliResource
from dbt.adapters.duckdb.connections import DuckDBConnectionManager

from orchestration.assets import build_dbt_assets, build_ingestion_asset, daily_partitions, dbt_project
from orchestration.resources import Era5PipelineResource
from orchestration.sensors import build_availability_sensor
from utils import pipeline
from utils.data_writer import daily_output_path

PARTITIONS = daily_partitions("2022-04-01")


@pytest.fixture(scope="module", autouse=True)
def dbt_manifest():
    """The dbt assets are defined from the manifest of the project."""
    dbt_project.preparer.prepare(dbt_project)


@pytest.fixture
def config_path(tmp_path, monkeypatch):
    config = {
        "bucket_path": "bucket/",
        "output_path": str(tmp_path / "output"),
        "query_output_path": str(tmp_path / "queries"),
        "h3_lookup_path": str(tmp_path / "output" / "h3_lookup"),
        "h3_resolution": 5,
        "variables": ["total_precipitation", "2m_temperature"],
        "file_name": "surface.nc",
        "partitions_start_date": "2022-04-01",
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    monkeypatch.setenv("ERA5_CONFIG", str(path))
    return str(path)


@pytest.fixture
def source(monkeypatch):
    fake = FakeSource()
    monkeypatch.setattr(pipeline, "build_source", lambda config: fake)
    return fake


def materialize(assets, config_path, partition_key="2022-04-02", **kwargs):
    resources = {"era5": Era5PipelineResource(config_path=config_path), "dbt": DbtCliResource(project_dir=dbt_project)}
    try:
        return dg.materialize(assets, partition_key=partition_key, resources=resources, **kwargs)
    finally:
        DuckDBConnectionManager.close_all_connections()


def test_ingestion_asset_writes_the_partition_and_reports_metadata(config_path, source, tmp_path):
    asset = build_ingestion_asset("2m_temperature", PARTITIONS)

    result = materialize([asset], config_path)

    assert result.success
    assert source.calls == [("2m_temperature", (2022, 4, 2))]
    assert os.path.exists(daily_output_path(str(tmp_path / "output"), "2m_temperature", 2022, 4, 2))
    metadata = result.asset_materializations_for_node(asset.op.name)[0].metadata
    assert metadata["rows"].value == 24 * 4
    assert metadata["missing_fraction"].value == 0
    [check] = result.get_asset_check_evaluations()
    assert check.check_name == "data_contract" and check.passed


def test_contract_violation_fails_the_asset_without_writing(config_path, source, tmp_path):
    original_read = source.read
    source.read = lambda field, *date: original_read(field, *date) * 2  # ~560 K: outside the valid range

    result = materialize([build_ingestion_asset("2m_temperature", PARTITIONS)], config_path, raise_on_error=False)

    assert not result.success
    assert "violates its data contract" in str(result.get_step_failure_events()[0].event_specific_data.error)
    assert not os.path.exists(daily_output_path(str(tmp_path / "output"), "2m_temperature", 2022, 4, 2))


def test_definitions_load_with_the_lineage_from_ingestion_to_marts(config_path):
    definitions = importlib.reload(importlib.import_module("orchestration.definitions")).defs

    graph = definitions.resolve_asset_graph()
    fct = dg.AssetKey(["marts", "fct_daily_cell_weather"])
    staging = dg.AssetKey(["staging", "stg_era5__temperature"])
    assert fct in graph.get_all_asset_keys()
    assert dg.AssetKey(["era5", "2m_temperature"]) in graph.get(staging).parent_keys
    assert definitions.resolve_job_def("era5_daily").partitions_def == PARTITIONS
    assert {check.name for check in graph.asset_check_keys if check.asset_key == fct} >= {
        "unique_combination_of_columns_fct_daily_cell_weather_date__h3_index"
    }


@pytest.mark.integration
def test_daily_job_runs_ingestion_then_dbt(config_path, source, tmp_path, monkeypatch):
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "warehouse.duckdb"))
    assets = [build_ingestion_asset(name, PARTITIONS) for name in ("total_precipitation", "2m_temperature")]

    result = materialize([*assets, build_dbt_assets(PARTITIONS)], config_path)

    assert result.success
    materialized = {event.asset_key for event in result.get_asset_materialization_events()}
    assert dg.AssetKey(["marts", "fct_heatwave_events"]) in materialized
    assert all(check.passed for check in result.get_asset_check_evaluations())


class PublishedUntil:
    """Source of which only the days up to the given one are published."""

    def __init__(self, last_day):
        self.last_day = last_day

    def available(self, field, year, month, day):
        return (year, month, day) <= self.last_day


def evaluate_sensor(config_path, monkeypatch, last_day, cursor=None):
    monkeypatch.setattr(pipeline, "build_source", lambda config: PublishedUntil(last_day))
    sensor = build_availability_sensor("era5_daily", ["total_precipitation", "2m_temperature"], PARTITIONS)
    context = dg.build_sensor_context(cursor=cursor, resources={"era5": Era5PipelineResource(config_path=config_path)})
    return list(sensor(context)), context.cursor


def test_sensor_requests_the_published_days_after_its_cursor(config_path, monkeypatch):
    requests, cursor = evaluate_sensor(config_path, monkeypatch, (2022, 4, 5), cursor="2022-04-02")

    assert [request.partition_key for request in requests] == ["2022-04-03", "2022-04-04", "2022-04-05"]
    assert cursor == "2022-04-05"


def test_sensor_skips_when_nothing_new_is_published(config_path, monkeypatch):
    requests, cursor = evaluate_sensor(config_path, monkeypatch, (2022, 4, 5), cursor="2022-04-05")

    assert len(requests) == 1 and isinstance(requests[0], dg.SkipReason)
    assert cursor == "2022-04-05"
