import os

import pytest
import yaml

from utils.config_loader import load_configuration

BASE_CONFIG = {
    "bucket_path": "bucket/",
    "output_path": "../output",
    "query_output_path": "../output/queries",
    "h3_lookup_path": "../output/h3_lookup",
    "h3_resolution": 5,
    "variables": ["total_precipitation", "2m_temperature"],
    "file_name": "surface.nc",
}


def write_config(tmp_path, content):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    path = config_dir / "config.yaml"
    path.write_text(yaml.safe_dump(content))
    return str(path)


def test_relative_paths_are_resolved_against_config_dir(tmp_path, monkeypatch):
    path = write_config(tmp_path, BASE_CONFIG)
    monkeypatch.chdir(tmp_path / "config")  # the working directory must not matter

    config = load_configuration(path)

    assert config["output_path"] == os.path.normpath(str(tmp_path / "output"))
    assert config["query_output_path"] == os.path.normpath(str(tmp_path / "output" / "queries"))
    assert config["max_retries"] == 3
    assert config["queries"] == []


def test_missing_keys_raise(tmp_path):
    content = {k: v for k, v in BASE_CONFIG.items() if k != "h3_resolution"}
    with pytest.raises(KeyError, match="h3_resolution"):
        load_configuration(write_config(tmp_path, content))


def test_unknown_variable_raises(tmp_path):
    with pytest.raises(ValueError, match="10m_wind"):
        load_configuration(write_config(tmp_path, {**BASE_CONFIG, "variables": ["10m_wind"]}))


def test_query_output_inside_a_dataset_raises(tmp_path):
    content = {**BASE_CONFIG, "query_output_path": "../output/2m_temperature/queries"}
    with pytest.raises(ValueError, match="2m_temperature"):
        load_configuration(write_config(tmp_path, content))


def test_project_config_is_valid():
    project_config = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
    config = load_configuration(project_config)
    assert config["variables"] == ["total_precipitation", "2m_temperature"]
    assert {q["variable"] for q in config["queries"]} == set(config["variables"])


def test_defaults_of_the_optional_keys(tmp_path):
    config = load_configuration(write_config(tmp_path, BASE_CONFIG))

    assert config["source"] == "netcdf"
    assert config["sink"] == "parquet"
    assert config["workers"] == 1
    assert config["storage_options"] == {"token": "anon"}
    assert config["iceberg_warehouse_path"] == os.path.join(config["output_path"], "iceberg")


def test_iceberg_warehouse_path_is_resolved_against_config_dir(tmp_path):
    config = load_configuration(write_config(tmp_path, {**BASE_CONFIG, "iceberg_warehouse_path": "../lake"}))
    assert config["iceberg_warehouse_path"] == os.path.normpath(str(tmp_path / "lake"))


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [("source", "grib", "source 'grib'"), ("sink", "delta", "sink 'delta'"), ("workers", 0, "workers")],
)
def test_invalid_choices_raise(tmp_path, key, value, message):
    with pytest.raises(ValueError, match=message):
        load_configuration(write_config(tmp_path, {**BASE_CONFIG, key: value}))


def test_defaults_are_not_shared_between_configurations(tmp_path):
    first = load_configuration(write_config(tmp_path, BASE_CONFIG))
    first["queries"].append({"name": "x"})
    first["storage_options"]["token"] = "secret"

    (tmp_path / "config").rename(tmp_path / "old")
    second = load_configuration(write_config(tmp_path, BASE_CONFIG))
    assert second["queries"] == []
    assert second["storage_options"] == {"token": "anon"}
