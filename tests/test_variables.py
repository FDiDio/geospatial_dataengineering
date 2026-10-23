import numpy as np
import pytest
import xarray as xr
from conftest import make_era5_df, make_precipitation_df, make_temperature_df

from utils.variables import (
    VARIABLES,
    SourceField,
    clean_precipitation_values,
    clean_relative_humidity_values,
    clean_temperature_values,
    clean_wind_speed_values,
    get_variable,
    relative_humidity,
    wind_speed,
)


def test_clean_precipitation_values():
    df = make_precipitation_df(["2022-01-01"]).head(4).copy()
    df["tp"] = [1e-16, -1.863e-9, 0.002, np.nan]  # -1.863e-9: a zero of the Zarr store
    result = clean_precipitation_values(df)["tp"].tolist()
    assert result[:3] == [0, 0, 0.002]
    assert np.isnan(result[3])


def test_clean_temperature_masks_implausible_values():
    df = make_temperature_df(["2022-01-01"]).head(4).copy()
    df["t2m"] = [100.0, 250.0, 300.0, 400.0]
    result = clean_temperature_values(df)["t2m"].tolist()
    assert np.isnan(result[0]) and np.isnan(result[3])
    assert result[1:3] == [250.0, 300.0]


def test_clean_temperature_keeps_negligible_values():
    # Unlike precipitation, temperature values close to zero are not set to zero
    df = make_temperature_df(["2022-01-01"]).head(1).copy()
    df["t2m"] = [273.15]
    assert clean_temperature_values(df)["t2m"].tolist() == [pytest.approx(273.15)]


def test_report_units():
    assert get_variable("total_precipitation").to_report_unit(0.0125) == pytest.approx(12.5)
    assert get_variable("2m_temperature").to_report_unit(273.15) == pytest.approx(0)


def test_temperature_cannot_be_summed():
    assert "sum" not in get_variable("2m_temperature").aggregations


def test_unknown_variable_raises():
    with pytest.raises(ValueError, match="10m_wind"):
        get_variable("10m_wind")


def test_fields_of_a_variable_read_as_is():
    precipitation = get_variable("total_precipitation")
    assert precipitation.fields == (SourceField("total_precipitation", "tp"),)


def test_wind_speed_from_components():
    inputs = xr.Dataset({"u10": ("x", [3.0, 0.0, -6.0]), "v10": ("x", [4.0, -2.0, 8.0])})
    assert wind_speed(inputs).values.tolist() == pytest.approx([5.0, 2.0, 10.0])


def test_relative_humidity_is_100_at_saturation_and_drops_with_the_dewpoint():
    inputs = xr.Dataset({"t2m": ("x", [293.15, 293.15, 303.15]), "d2m": ("x", [293.15, 283.15, 283.15])})
    rh = relative_humidity(inputs).values
    assert rh[0] == pytest.approx(100.0)
    # 20 °C with a 10 °C dewpoint is ~52 %, 30 °C with the same dewpoint ~29 %
    assert rh[1] == pytest.approx(52.5, abs=0.5)
    assert rh[2] == pytest.approx(28.8, abs=0.5)


def test_relative_humidity_is_clipped_to_100():
    df = make_era5_df(["2022-01-01"], "rh2m", 100.4).head(2).copy()
    df["rh2m"] = np.array([100.4, -0.1], dtype="float32")
    assert clean_relative_humidity_values(df)["rh2m"].tolist() == [100.0, 0.0]


def test_implausible_wind_speed_is_masked():
    df = make_era5_df(["2022-01-01"], "ws10", 5.0).head(2).copy()
    df["ws10"] = np.array([5.0, 500.0], dtype="float32")
    result = clean_wind_speed_values(df)["ws10"].tolist()
    assert result[0] == 5.0 and np.isnan(result[1])


def test_every_variable_has_a_valid_range_and_aggregations():
    for variable in VARIABLES.values():
        low, high = variable.valid_range
        assert low < high
        assert variable.aggregations
