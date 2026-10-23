import fsspec
import numpy as np
import pytest
from conftest import make_precipitation_df, make_temperature_df
from utils.data_extractor import extract_variable_data, generate_date_range
from utils.variables import get_variable

BUCKET = "memory://bucket/raw/"
PRECIPITATION = get_variable("total_precipitation")
TEMPERATURE = get_variable("2m_temperature")


@pytest.fixture
def memory_fs():
    fs = fsspec.filesystem("memory")
    yield fs
    fs.store.clear()


def write_netcdf(fs, df, path):
    dataset = df.set_index(["time", "latitude", "longitude"]).to_xarray()
    with fs.open(path, "wb") as f:
        f.write(dataset.to_netcdf(engine="scipy"))


def test_generate_date_range_is_inclusive_and_crosses_months():
    assert generate_date_range("20220130", "20220202") == [(2022, 1, 30), (2022, 1, 31), (2022, 2, 1), (2022, 2, 2)]


def test_generate_date_range_rejects_inverted_range():
    with pytest.raises(ValueError):
        generate_date_range("20220102", "20220101")


def test_extract_reads_daily_precipitation_file(memory_fs):
    source = make_precipitation_df(["2022-04-30"])
    write_netcdf(memory_fs, source, f"{BUCKET}2022/04/30/total_precipitation/surface.nc")

    df = extract_variable_data(BUCKET, 2022, 4, 30, PRECIPITATION, "surface.nc", fs=memory_fs)

    assert len(df) == len(source)
    assert set(df.columns) == {"time", "latitude", "longitude", "tp"}


def test_extract_reads_and_cleans_daily_temperature_file(memory_fs):
    source = make_temperature_df(["2022-04-30"])
    source.loc[0, "t2m"] = 999.0  # implausible value
    write_netcdf(memory_fs, source, f"{BUCKET}2022/04/30/2m_temperature/surface.nc")

    df = extract_variable_data(BUCKET, 2022, 4, 30, TEMPERATURE, "surface.nc", fs=memory_fs)

    assert set(df.columns) == {"time", "latitude", "longitude", "t2m"}
    assert df["t2m"].isna().sum() == 1
    assert df["t2m"].max() < 350


def test_extract_wrong_variable_in_file_raises(memory_fs):
    # A temperature folder containing a precipitation file
    write_netcdf(memory_fs, make_precipitation_df(["2022-04-30"]), f"{BUCKET}2022/04/30/2m_temperature/surface.nc")
    with pytest.raises(ValueError, match="2m_temperature"):
        extract_variable_data(BUCKET, 2022, 4, 30, TEMPERATURE, "surface.nc", fs=memory_fs)


def test_extract_missing_day_is_not_retried(memory_fs, monkeypatch):
    calls = []
    original_glob = memory_fs.glob

    def counting_glob(path):
        calls.append(path)
        return original_glob(path)

    monkeypatch.setattr(memory_fs, "glob", counting_glob)
    with pytest.raises(FileNotFoundError):
        extract_variable_data(BUCKET, 2022, 1, 1, PRECIPITATION, "surface.nc",
                              max_retries=3, retry_delay=0, fs=memory_fs)
    assert len(calls) == 1


def test_extract_retries_transient_errors(memory_fs, monkeypatch):
    write_netcdf(memory_fs, make_precipitation_df(["2022-04-30"]), f"{BUCKET}2022/04/30/total_precipitation/surface.nc")
    original_glob = memory_fs.glob
    failures = [OSError("connection reset")]

    def flaky_glob(path):
        if failures:
            raise failures.pop()
        return original_glob(path)

    monkeypatch.setattr(memory_fs, "glob", flaky_glob)
    df = extract_variable_data(BUCKET, 2022, 4, 30, PRECIPITATION, "surface.nc",
                               max_retries=2, retry_delay=0, fs=memory_fs)
    assert not df.empty


def test_extract_gives_up_after_max_retries(memory_fs, monkeypatch):
    def broken_glob(path):
        raise OSError("service unavailable")

    monkeypatch.setattr(memory_fs, "glob", broken_glob)
    with pytest.raises(OSError):
        extract_variable_data(BUCKET, 2022, 4, 30, PRECIPITATION, "surface.nc",
                              max_retries=2, retry_delay=0, fs=memory_fs)
