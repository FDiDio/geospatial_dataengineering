import numpy as np
import pytest
from conftest import make_precipitation_df, make_temperature_df

from utils.h3_processor import map_h3_indices_to_dataframe, precompute_h3_index_lookup
from utils.quality import DataContractError, validate_variable_day
from utils.variables import get_variable

TEMPERATURE = get_variable("2m_temperature")


def indexed(df, resolution=5):
    return map_h3_indices_to_dataframe(df, precompute_h3_index_lookup(df, resolution))


def validate(df, variable=TEMPERATURE, day=30, **kwargs):
    return validate_variable_day(df, variable, 2022, 4, day, h3_resolution=5, **kwargs)


@pytest.fixture
def temperature_day():
    return indexed(make_temperature_df(["2022-04-30"]))


def test_valid_day_passes(temperature_day):
    assert validate(temperature_day) is temperature_day
    validate(indexed(make_precipitation_df(["2022-04-30"])), get_variable("total_precipitation"))


def test_value_outside_the_valid_range_fails(temperature_day):
    temperature_day.loc[3, "t2m"] = np.float32(400.0)
    with pytest.raises(DataContractError, match=r"t2m: in_range\(150.0, 350.0\) \(1 failures\)"):
        validate(temperature_day)


def test_rows_of_another_day_fail(temperature_day):
    with pytest.raises(DataContractError, match="time: in_range"):
        validate(temperature_day, day=29)


def test_missing_hours_fail(temperature_day):
    with pytest.raises(DataContractError, match="complete hourly grid"):
        validate(temperature_day[temperature_day["time"].dt.hour != 12])


def test_missing_grid_points_fail(temperature_day):
    partial = temperature_day.drop(index=temperature_day.index[:5])
    with pytest.raises(DataContractError, match="complete hourly grid"):
        validate(partial)


def test_too_many_missing_values_fail(temperature_day):
    temperature_day.loc[:9, "t2m"] = np.nan  # 10 of 96 values
    validate(temperature_day, max_null_fraction=0.2)
    with pytest.raises(DataContractError, match=r"at most 1.0% missing t2m values"):
        validate(temperature_day)


def test_h3_index_at_another_resolution_fails():
    with pytest.raises(DataContractError, match="H3 cell identifiers at resolution 5"):
        validate(indexed(make_temperature_df(["2022-04-30"]), resolution=6))


def test_float64_values_fail(temperature_day):
    with pytest.raises(DataContractError, match="dtype"):
        validate(temperature_day.astype({"t2m": "float64"}))


def test_unexpected_columns_fail(temperature_day):
    with pytest.raises(DataContractError, match="column_in_schema"):
        validate(temperature_day.assign(tp=0.0))


def test_every_violation_is_reported(temperature_day):
    temperature_day.loc[0, "t2m"] = np.float32(10.0)
    temperature_day.loc[1, "latitude"] = np.float32(95.0)
    with pytest.raises(DataContractError) as error:
        validate(temperature_day)
    assert "t2m:" in str(error.value) and "latitude:" in str(error.value)
