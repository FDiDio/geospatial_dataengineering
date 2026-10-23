import logging
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

# Precipitation values below this threshold (0.00001 mm) are numerical noise and are set to zero.
# The GRIB packing of the Zarr store encodes no precipitation as -1.86e-9 m instead of 0.
NEGLIGIBLE_THRESHOLD = 1e-8

# Physically plausible ranges, used both to mask implausible values and by the data contract (utils.quality)
PRECIPITATION_RANGE_M = (0.0, 0.5)  # hourly accumulation; the 1-hour world record is ~0.3 m
TEMPERATURE_RANGE_K = (150.0, 350.0)  # record lows/highs are ~184 K and ~330 K
WIND_SPEED_RANGE_MS = (0.0, 120.0)  # 10m wind speed; the strongest gust ever measured is ~113 m/s
RELATIVE_HUMIDITY_RANGE = (0.0, 100.0)

KELVIN_OFFSET = 273.15

# Magnus formula coefficients for the saturation vapour pressure over water (Alduchov & Eskridge, 1996)
MAGNUS_B, MAGNUS_C = 17.625, 243.04


def clean_precipitation_values(df, column="tp"):
    """
    Set negligible total precipitation values to zero.

    Args:
        df (pd.DataFrame): DataFrame containing the precipitation column.
        column (str): Name of the precipitation column.

    Returns:
        pd.DataFrame: The same DataFrame with negligible values replaced.
    """
    df[column] = df[column].mask(df[column].abs() < NEGLIGIBLE_THRESHOLD, 0)
    return df


def mask_implausible_values(df, column, valid_range):
    """
    Replace the values outside a physically plausible range with NaN.

    Args:
        df (pd.DataFrame): DataFrame containing the column.
        column (str): Name of the column.
        valid_range (tuple): Inclusive (low, high) bounds.

    Returns:
        pd.DataFrame: The same DataFrame with implausible values masked.
    """
    low, high = valid_range
    implausible = (df[column] < low) | (df[column] > high)
    if implausible.any():
        logger.warning("Masking %d implausible %s values outside %s-%s", implausible.sum(), column, low, high)
        df[column] = df[column].mask(implausible)
    return df


def clean_temperature_values(df, column="t2m"):
    """
    Replace physically implausible 2m temperature values with NaN.

    Args:
        df (pd.DataFrame): DataFrame containing the temperature column, in Kelvin.
        column (str): Name of the temperature column.

    Returns:
        pd.DataFrame: The same DataFrame with implausible values masked.
    """
    return mask_implausible_values(df, column, TEMPERATURE_RANGE_K)


def clean_wind_speed_values(df, column="ws10"):
    """Replace physically implausible 10m wind speed values with NaN."""
    return mask_implausible_values(df, column, WIND_SPEED_RANGE_MS)


def clean_relative_humidity_values(df, column="rh2m"):
    """
    Clip relative humidity to 0-100 %.

    The dewpoint can slightly exceed the temperature in the model output (supersaturation
    and rounding), which gives values just above 100 %.
    """
    df[column] = df[column].clip(*RELATIVE_HUMIDITY_RANGE)
    return df


def wind_speed(inputs: xr.Dataset) -> xr.DataArray:
    """Wind speed from its eastward (u10) and northward (v10) components, in m/s."""
    return xr.ufuncs.hypot(inputs["u10"], inputs["v10"])


def relative_humidity(inputs: xr.Dataset) -> xr.DataArray:
    """Relative humidity from the 2m temperature (t2m) and dewpoint (d2m), in %, with the Magnus formula."""
    temperature = inputs["t2m"] - KELVIN_OFFSET
    dewpoint = inputs["d2m"] - KELVIN_OFFSET
    return 100 * xr.ufuncs.exp(
        MAGNUS_B * dewpoint / (MAGNUS_C + dewpoint) - MAGNUS_B * temperature / (MAGNUS_C + temperature)
    )


@dataclass(frozen=True)
class SourceField:
    """
    An ERA5 single level field as stored in the sources.

    Attributes:
        name: Folder name in the raw bucket, and variable name in the Zarr store.
        short_name: Variable name inside the daily NetCDF files.
    """

    name: str
    short_name: str


@dataclass(frozen=True)
class VariableSpec:
    """
    Variable-specific logic of an ERA5 single level variable.

    Attributes:
        name: Dataset name; for a variable read as is, also its folder name in the bucket.
        short_name: Name of the Parquet column (and of the variable inside the NetCDF file).
        source_unit: Unit stored in the Parquet files.
        report_unit: Unit used when reporting aggregated values.
        aggregations: Aggregations that are meaningful for the variable.
        clean: Cleaning applied to the extracted DataFrame.
        to_report_unit: Conversion from source_unit to report_unit.
        valid_range: Inclusive bounds of the stored values, enforced by the data contract.
        inputs: Source fields of a derived variable; empty when the variable is read as is.
        derive: Computation of a derived variable from its inputs, keyed by short name.
    """

    name: str
    short_name: str
    source_unit: str
    report_unit: str
    aggregations: tuple[str, ...]
    clean: Callable[[pd.DataFrame, str], pd.DataFrame]
    to_report_unit: Callable[[float], float]
    valid_range: tuple[float, float]
    inputs: tuple[SourceField, ...] = ()
    derive: Callable[[xr.Dataset], xr.DataArray] | None = None

    @property
    def fields(self) -> tuple[SourceField, ...]:
        """Source fields to read: the inputs of a derived variable, otherwise the variable itself."""
        return self.inputs or (SourceField(self.name, self.short_name),)


VARIABLES = {
    spec.name: spec
    for spec in (
        # Hourly accumulation: summing over time gives the total precipitation
        VariableSpec(
            name="total_precipitation",
            short_name="tp",
            source_unit="m",
            report_unit="mm",
            aggregations=("sum",),
            clean=clean_precipitation_values,
            to_report_unit=lambda value: value * 1000,
            valid_range=PRECIPITATION_RANGE_M,
        ),
        # Instantaneous value: a sum over time is meaningless, statistics are reported instead
        VariableSpec(
            name="2m_temperature",
            short_name="t2m",
            source_unit="K",
            report_unit="°C",
            aggregations=("mean", "min", "max"),
            clean=clean_temperature_values,
            to_report_unit=lambda value: value - KELVIN_OFFSET,
            valid_range=TEMPERATURE_RANGE_K,
        ),
        # Derived variables, computed from two ERA5 fields
        VariableSpec(
            name="10m_wind_speed",
            short_name="ws10",
            source_unit="m/s",
            report_unit="m/s",
            aggregations=("mean", "min", "max"),
            clean=clean_wind_speed_values,
            to_report_unit=lambda value: value,
            valid_range=WIND_SPEED_RANGE_MS,
            inputs=(
                SourceField("10m_u_component_of_wind", "u10"),
                SourceField("10m_v_component_of_wind", "v10"),
            ),
            derive=wind_speed,
        ),
        VariableSpec(
            name="2m_relative_humidity",
            short_name="rh2m",
            source_unit="%",
            report_unit="%",
            aggregations=("mean", "min", "max"),
            clean=clean_relative_humidity_values,
            to_report_unit=lambda value: value,
            valid_range=RELATIVE_HUMIDITY_RANGE,
            inputs=(
                SourceField("2m_temperature", "t2m"),
                SourceField("2m_dewpoint_temperature", "d2m"),
            ),
            derive=relative_humidity,
        ),
    )
}


def get_variable(name):
    """
    Return the specification of a supported variable.

    Args:
        name (str): Variable name, e.g. "total_precipitation".

    Returns:
        VariableSpec: Specification of the variable.
    """
    try:
        return VARIABLES[name]
    except KeyError:
        raise ValueError(f"Unsupported variable '{name}', supported: {', '.join(VARIABLES)}") from None
