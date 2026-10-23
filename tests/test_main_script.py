import os

import pytest
import yaml
from conftest import FakeSource

import main_script
from utils import pipeline
from utils.data_writer import daily_output_path
from utils.iceberg_sink import IcebergSink, local_catalog_properties


def write_config(tmp_path, **overrides):
    config = {
        "bucket_path": "bucket/",
        "output_path": "output",
        "query_output_path": "queries",
        "h3_lookup_path": "lookup",
        "h3_resolution": 5,
        "variables": ["total_precipitation", "2m_temperature"],
        "file_name": "surface.nc",
        "max_retries": 1,
        "queries": [
            {"name": "all_april", "variable": "total_precipitation", "start": "2022-04-01", "end": "2022-04-30"},
            {"name": "april_t", "variable": "2m_temperature", "start": "2022-04-01", "end": "2022-04-30"},
        ],
        **overrides,
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    return str(path)


@pytest.fixture
def use_source(monkeypatch):
    """Make the pipeline read from the given fake source."""

    def install(source):
        monkeypatch.setattr(pipeline, "build_source", lambda config: source)
        return source

    return install


def run(config, *extra):
    return main_script.main(["--config", config, "--start_date", "20220401", "--end_date", "20220403", *extra])


def test_all_configured_variables_are_processed(tmp_path, use_source):
    source = use_source(FakeSource())

    assert run(write_config(tmp_path)) == 0
    assert len(source.calls) == 6
    for variable in ("total_precipitation", "2m_temperature"):
        assert os.path.exists(daily_output_path(str(tmp_path / "output"), variable, 2022, 4, 3))
    assert os.path.exists(tmp_path / "queries" / "april_t.parquet")


def test_failed_variable_day_does_not_stop_the_run(tmp_path, use_source):
    source = use_source(FakeSource(missing={("2m_temperature", (2022, 4, 2))}))

    exit_code = run(write_config(tmp_path))

    assert exit_code == 1
    assert len(source.calls) == 6
    output = str(tmp_path / "output")
    assert os.path.exists(daily_output_path(output, "total_precipitation", 2022, 4, 2))
    assert not os.path.exists(daily_output_path(output, "2m_temperature", 2022, 4, 2))
    assert os.path.exists(daily_output_path(output, "2m_temperature", 2022, 4, 3))
    assert os.path.exists(tmp_path / "queries" / "all_april.parquet")


def test_rerun_resumes_from_missing_variable_days(tmp_path, use_source):
    config = write_config(tmp_path)
    use_source(FakeSource(missing={("2m_temperature", (2022, 4, 2))}))
    run(config, "--skip_queries")

    source = use_source(FakeSource())
    assert run(config, "--skip_queries") == 0
    assert source.calls == [("2m_temperature", (2022, 4, 2))]

    source = use_source(FakeSource())
    run(config, "--skip_queries", "--overwrite")
    assert len(source.calls) == 6


def test_variables_argument_selects_a_subset(tmp_path, use_source):
    source = use_source(FakeSource())

    assert run(write_config(tmp_path), "--variables", "2m_temperature") == 0
    assert {name for name, _ in source.calls} == {"2m_temperature"}
    # The precipitation queries are skipped, there is no precipitation data
    assert not os.path.exists(tmp_path / "queries" / "all_april.parquet")
    assert os.path.exists(tmp_path / "queries" / "april_t.parquet")


def test_parallel_workers_process_every_variable_day(tmp_path, use_source):
    source = use_source(FakeSource(missing={("total_precipitation", (2022, 4, 1))}))

    assert run(write_config(tmp_path), "--workers", "4", "--skip_queries") == 1

    assert len(source.calls) == 6
    output = str(tmp_path / "output")
    written = [(v, d) for v in ("total_precipitation", "2m_temperature") for d in (1, 2, 3)
               if os.path.exists(daily_output_path(output, v, 2022, 4, d))]  # fmt: skip
    assert len(written) == 5
    # The shared H3 lookup table is computed once
    assert os.listdir(tmp_path / "lookup") == ["h3_lookup_res5.parquet"]


def test_derived_variable_end_to_end(tmp_path, use_source):
    use_source(FakeSource())

    assert run(write_config(tmp_path, queries=[]), "--variables", "2m_relative_humidity") == 0
    assert os.path.exists(daily_output_path(str(tmp_path / "output"), "2m_relative_humidity", 2022, 4, 2))


def test_contract_violation_fails_the_variable_day(tmp_path, use_source):
    source = use_source(FakeSource())
    original_read = source.read

    def read_with_gaps(field, year, month, day):
        # Half of the temperature grid is missing, far above the allowed share of missing values
        data = original_read(field, year, month, day)
        return data.where(data.latitude > 47.3) if field.name == "2m_temperature" else data

    source.read = read_with_gaps

    assert run(write_config(tmp_path), "--skip_queries") == 1
    output = str(tmp_path / "output")
    assert not os.path.exists(daily_output_path(output, "2m_temperature", 2022, 4, 1))
    assert os.path.exists(daily_output_path(output, "total_precipitation", 2022, 4, 1))


def test_iceberg_sink_end_to_end(tmp_path, use_source):
    use_source(FakeSource())
    config = write_config(tmp_path, sink="iceberg")

    assert run(config, "--workers", "3") == 0
    assert os.path.exists(tmp_path / "queries" / "april_t.parquet")

    sink = IcebergSink(local_catalog_properties(str(tmp_path / "output" / "iceberg")))
    assert sink.load_table("2m_temperature").scan().to_arrow().num_rows == 3 * 24 * 4

    # Reprocessing replaces the days instead of appending them
    source = use_source(FakeSource())
    assert run(config, "--skip_queries") == 0
    assert source.calls == []
    run(config, "--skip_queries", "--overwrite")
    assert sink.load_table("2m_temperature").scan().to_arrow().num_rows == 3 * 24 * 4


def test_legacy_config_file_argument_is_accepted():
    args = main_script.parse_arguments(["--config_file", "custom.yaml"])
    assert args.config == "custom.yaml"
