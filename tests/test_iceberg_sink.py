from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import make_temperature_df

from utils.h3_processor import map_h3_indices_to_dataframe, precompute_h3_index_lookup
from utils.iceberg_sink import IcebergSink, local_catalog_properties


def temperature_day(day, kelvin=283.15):
    df = make_temperature_df([f"2022-04-{day:02d}"], kelvin=kelvin)
    return map_h3_indices_to_dataframe(df, precompute_h3_index_lookup(df, 5))


@pytest.fixture
def sink(tmp_path):
    return IcebergSink(local_catalog_properties(str(tmp_path / "warehouse")))


def rows(sink, *conditions):
    return sink.read("2m_temperature", list(conditions)).to_pandas()


def test_table_is_partitioned_by_day(sink):
    sink.write(temperature_day(1), "2m_temperature", 2022, 4, 1)

    table = sink.load_table("2m_temperature")
    assert table.name() == ("era5", "2m_temperature")
    assert [field.name for field in table.spec().fields] == ["day"]


def test_rewriting_a_day_replaces_it(sink):
    sink.write(temperature_day(1), "2m_temperature", 2022, 4, 1)
    sink.write(temperature_day(2), "2m_temperature", 2022, 4, 2)
    sink.write(temperature_day(2, kelvin=300.0), "2m_temperature", 2022, 4, 2)

    df = rows(sink)
    assert len(df) == 2 * 24 * 4
    assert df.groupby(df["time"].dt.day)["t2m"].mean().to_dict() == pytest.approx({1: 283.9, 2: 300.75})
    # Each write is one atomic snapshot: readers can travel back to the previous version of the day
    assert len(sink.load_table("2m_temperature").snapshots()) >= 3


def test_exists_tracks_the_written_days(sink, tmp_path):
    assert not sink.exists("2m_temperature", 2022, 4, 1)
    sink.write(temperature_day(1), "2m_temperature", 2022, 4, 1)
    assert sink.exists("2m_temperature", 2022, 4, 1)
    assert not sink.exists("2m_temperature", 2022, 4, 2)

    # A new process sees the days written by the previous ones
    reopened = IcebergSink(local_catalog_properties(str(tmp_path / "warehouse")))
    assert reopened.exists("2m_temperature", 2022, 4, 1)


def test_read_prunes_with_conditions(sink):
    for day in (1, 2, 3):
        sink.write(temperature_day(day), "2m_temperature", 2022, 4, day)

    df = rows(sink, ("time", ">=", temperature_day(2)["time"].min().to_pydatetime()), ("latitude", "==", 47.5))
    assert sorted(df["time"].dt.day.unique()) == [2, 3]
    assert set(df["latitude"]) == {47.5}


def test_unknown_variable_reads_none(sink):
    assert sink.read("total_precipitation", []) is None


def test_concurrent_writers_do_not_lose_days(sink):
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda day: sink.write(temperature_day(day), "2m_temperature", 2022, 4, day), range(1, 9)))

    assert sorted(rows(sink)["time"].dt.day.unique()) == list(range(1, 9))
