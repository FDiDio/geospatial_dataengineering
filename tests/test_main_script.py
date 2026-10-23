import os
import yaml
import main_script
from conftest import make_precipitation_df, make_temperature_df
from utils.data_writer import daily_output_path

MAKERS = {"total_precipitation": make_precipitation_df, "2m_temperature": make_temperature_df}


def write_config(tmp_path):
    config = {
        "bucket_path": "bucket/", "output_path": "output", "query_output_path": "queries",
        "h3_lookup_path": "lookup", "h3_resolution": 5,
        "variables": ["total_precipitation", "2m_temperature"],
        "file_name": "surface.nc", "max_retries": 1,
        "queries": [
            {"name": "all_april", "variable": "total_precipitation", "start": "2022-04-01", "end": "2022-04-30"},
            {"name": "april_t", "variable": "2m_temperature", "start": "2022-04-01", "end": "2022-04-30"},
        ],
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    return str(path)


def fake_extractor(missing=()):
    calls = []

    def extract(bucket_path, year, month, day, variable, file_name, max_retries=3):
        calls.append((variable.name, (year, month, day)))
        if (variable.name, (year, month, day)) in missing:
            raise FileNotFoundError(f"No {variable.name} files found for {year}-{month:02d}-{day:02d}.")
        return MAKERS[variable.name]([f"{year}-{month:02d}-{day:02d}"])

    return extract, calls


def run(config, *extra):
    return main_script.main(["--config", config, "--start_date", "20220401", "--end_date", "20220403", *extra])


def test_all_configured_variables_are_processed(tmp_path, monkeypatch):
    extract, calls = fake_extractor()
    monkeypatch.setattr(main_script, "extract_variable_data", extract)

    assert run(write_config(tmp_path)) == 0
    assert len(calls) == 6
    for variable in MAKERS:
        assert os.path.exists(daily_output_path(str(tmp_path / "output"), variable, 2022, 4, 3))
    assert os.path.exists(tmp_path / "queries" / "april_t.parquet")


def test_failed_variable_day_does_not_stop_the_run(tmp_path, monkeypatch):
    extract, calls = fake_extractor(missing={("2m_temperature", (2022, 4, 2))})
    monkeypatch.setattr(main_script, "extract_variable_data", extract)

    exit_code = run(write_config(tmp_path))

    assert exit_code == 1
    assert len(calls) == 6
    output = str(tmp_path / "output")
    assert os.path.exists(daily_output_path(output, "total_precipitation", 2022, 4, 2))
    assert not os.path.exists(daily_output_path(output, "2m_temperature", 2022, 4, 2))
    assert os.path.exists(daily_output_path(output, "2m_temperature", 2022, 4, 3))
    assert os.path.exists(tmp_path / "queries" / "all_april.parquet")


def test_rerun_resumes_from_missing_variable_days(tmp_path, monkeypatch):
    config = write_config(tmp_path)
    extract, _ = fake_extractor(missing={("2m_temperature", (2022, 4, 2))})
    monkeypatch.setattr(main_script, "extract_variable_data", extract)
    run(config, "--skip_queries")

    extract, calls = fake_extractor()
    monkeypatch.setattr(main_script, "extract_variable_data", extract)
    assert run(config, "--skip_queries") == 0
    assert calls == [("2m_temperature", (2022, 4, 2))]

    extract, calls = fake_extractor()
    monkeypatch.setattr(main_script, "extract_variable_data", extract)
    run(config, "--skip_queries", "--overwrite")
    assert len(calls) == 6


def test_variables_argument_selects_a_subset(tmp_path, monkeypatch):
    extract, calls = fake_extractor()
    monkeypatch.setattr(main_script, "extract_variable_data", extract)

    assert run(write_config(tmp_path), "--variables", "2m_temperature") == 0
    assert {name for name, _ in calls} == {"2m_temperature"}
    # The precipitation queries are skipped, there is no precipitation data
    assert not os.path.exists(tmp_path / "queries" / "all_april.parquet")
    assert os.path.exists(tmp_path / "queries" / "april_t.parquet")


def test_legacy_config_file_argument_is_accepted():
    args = main_script.parse_arguments(["--config_file", "custom.yaml"])
    assert args.config == "custom.yaml"
