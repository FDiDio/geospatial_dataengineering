import numpy as np
import pytest
from conftest import make_precipitation_df, make_temperature_df

from utils.variables import clean_precipitation_values, clean_temperature_values, get_variable


def test_clean_precipitation_values():
    df = make_precipitation_df(["2022-01-01"]).head(4).copy()
    df["tp"] = [1e-16, -1e-16, 0.002, np.nan]
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
