import fsspec
import numpy as np
import pytest
from conftest import FakeSource, make_precipitation_df, make_temperature_df, to_data_array

from utils.data_extractor import extract_variable_data, generate_date_range
from utils.sources import NetCDFSource, ZarrSource
from utils.variables import get_variable

BUCKET = "memory://bucket/raw/"
PRECIPITATION = get_variable("total_precipitation")
TEMPERATURE = get_variable("2m_temperature")


@pytest.fixture
def memory_fs():
    fs = fsspec.filesystem("memory")
    yield fs
    fs.store.clear()


@pytest.fixture
def netcdf_source(memory_fs):
    return NetCDFSource(BUCKET, "surface.nc", fs=memory_fs)


def write_netcdf(fs, df, path):
    dataset = df.set_index(["time", "latitude", "longitude"]).to_xarray()
    with fs.open(path, "wb") as f:
        f.write(dataset.to_netcdf(engine="scipy"))


def test_generate_date_range_is_inclusive_and_crosses_months():
    assert generate_date_range("20220130", "20220202") == [(2022, 1, 30), (2022, 1, 31), (2022, 2, 1), (2022, 2, 2)]


def test_generate_date_range_rejects_inverted_range():
    with pytest.raises(ValueError):
        generate_date_range("20220102", "20220101")


def test_extract_reads_daily_precipitation_file(memory_fs, netcdf_source):
    source = make_precipitation_df(["2022-04-30"])
    write_netcdf(memory_fs, source, f"{BUCKET}2022/04/30/total_precipitation/surface.nc")

    df = extract_variable_data(netcdf_source, 2022, 4, 30, PRECIPITATION)

    assert len(df) == len(source)
    assert set(df.columns) == {"time", "latitude", "longitude", "tp"}
    assert df["tp"].dtype == np.float32


def test_extract_reads_and_cleans_daily_temperature_file(memory_fs, netcdf_source):
    source = make_temperature_df(["2022-04-30"])
    source.loc[0, "t2m"] = 999.0  # implausible value
    write_netcdf(memory_fs, source, f"{BUCKET}2022/04/30/2m_temperature/surface.nc")

    df = extract_variable_data(netcdf_source, 2022, 4, 30, TEMPERATURE)

    assert set(df.columns) == {"time", "latitude", "longitude", "t2m"}
    assert df["t2m"].isna().sum() == 1
    assert df["t2m"].max() < 350


def test_file_name_can_depend_on_the_date(memory_fs):
    write_netcdf(
        memory_fs, make_precipitation_df(["2022-04-30"]), f"{BUCKET}2022/04/30/total_precipitation/20220430.nc"
    )
    source = NetCDFSource(BUCKET, "{year}{month:02d}{day:02d}.nc", fs=memory_fs)
    assert not extract_variable_data(source, 2022, 4, 30, PRECIPITATION).empty


def test_extract_wrong_variable_in_file_raises(memory_fs, netcdf_source):
    # A temperature folder containing a precipitation file
    write_netcdf(memory_fs, make_precipitation_df(["2022-04-30"]), f"{BUCKET}2022/04/30/2m_temperature/surface.nc")
    with pytest.raises(ValueError, match="2m_temperature"):
        extract_variable_data(netcdf_source, 2022, 4, 30, TEMPERATURE)


def test_extract_missing_day_is_not_retried(memory_fs, netcdf_source, monkeypatch):
    calls = []
    original_glob = memory_fs.glob

    def counting_glob(path):
        calls.append(path)
        return original_glob(path)

    monkeypatch.setattr(memory_fs, "glob", counting_glob)
    with pytest.raises(FileNotFoundError):
        extract_variable_data(netcdf_source, 2022, 1, 1, PRECIPITATION, max_retries=3, retry_delay=0)
    assert len(calls) == 1


def test_extract_retries_transient_errors(memory_fs, netcdf_source, monkeypatch):
    write_netcdf(memory_fs, make_precipitation_df(["2022-04-30"]), f"{BUCKET}2022/04/30/total_precipitation/surface.nc")
    original_glob = memory_fs.glob
    failures = [OSError("connection reset")]

    def flaky_glob(path):
        if failures:
            raise failures.pop()
        return original_glob(path)

    monkeypatch.setattr(memory_fs, "glob", flaky_glob)
    df = extract_variable_data(netcdf_source, 2022, 4, 30, PRECIPITATION, max_retries=2, retry_delay=0)
    assert not df.empty


def test_extract_gives_up_after_max_retries(memory_fs, netcdf_source, monkeypatch):
    def broken_glob(path):
        raise OSError("service unavailable")

    monkeypatch.setattr(memory_fs, "glob", broken_glob)
    with pytest.raises(OSError):
        extract_variable_data(netcdf_source, 2022, 4, 30, PRECIPITATION, max_retries=2, retry_delay=0)


def test_derived_variable_reads_all_its_inputs():
    source = FakeSource()

    df = extract_variable_data(source, 2022, 4, 30, get_variable("10m_wind_speed"))

    assert {name for name, _ in source.calls} == {"10m_u_component_of_wind", "10m_v_component_of_wind"}
    assert set(df.columns) == {"time", "latitude", "longitude", "ws10"}
    assert df["ws10"].to_numpy() == pytest.approx(5.0)  # hypot(3, -4)


def test_missing_input_of_a_derived_variable_raises():
    source = FakeSource(missing={("2m_dewpoint_temperature", (2022, 4, 30))})
    with pytest.raises(FileNotFoundError, match="2m_dewpoint_temperature"):
        extract_variable_data(source, 2022, 4, 30, get_variable("2m_relative_humidity"))


@pytest.fixture
def zarr_store(tmp_path):
    days = ["2022-04-29", "2022-04-30", "2022-05-01"]
    dataset = to_data_array(make_temperature_df(days), "t2m").rename("2m_temperature").to_dataset()
    dataset["total_precipitation"] = to_data_array(make_precipitation_df(days), "tp")
    dataset.attrs["valid_time_stop"] = "2022-04-30"
    path = str(tmp_path / "era5.zarr")
    dataset.to_zarr(path, zarr_format=2)  # the format of the ARCO-ERA5 store
    return path


def test_zarr_source_reads_one_day(zarr_store):
    df = extract_variable_data(ZarrSource(zarr_store), 2022, 4, 29, TEMPERATURE)

    assert len(df) == 24 * 4
    assert df["time"].dt.day.unique().tolist() == [29]
    assert set(df.columns) == {"time", "latitude", "longitude", "t2m"}


def test_zarr_source_stops_at_the_last_published_day(zarr_store):
    # The time axis of the store contains days whose data is not published yet
    with pytest.raises(FileNotFoundError, match="ends on 2022-04-30"):
        extract_variable_data(ZarrSource(zarr_store), 2022, 5, 1, PRECIPITATION)


def test_zarr_source_unknown_field_raises(zarr_store):
    with pytest.raises(ValueError, match="10m_u_component_of_wind"):
        extract_variable_data(ZarrSource(zarr_store), 2022, 4, 30, get_variable("10m_wind_speed"))


def test_netcdf_availability(memory_fs, netcdf_source):
    write_netcdf(memory_fs, make_precipitation_df(["2022-04-30"]), f"{BUCKET}2022/04/30/total_precipitation/surface.nc")
    assert netcdf_source.available(PRECIPITATION.fields[0], 2022, 4, 30)
    assert not netcdf_source.available(PRECIPITATION.fields[0], 2022, 5, 1)


def test_zarr_availability_follows_the_last_published_day(zarr_store):
    source = ZarrSource(zarr_store)
    assert source.available(TEMPERATURE.fields[0], 2022, 4, 30)
    assert not source.available(TEMPERATURE.fields[0], 2022, 5, 1)
    assert not source.available(get_variable("10m_wind_speed").fields[0], 2022, 4, 30)
