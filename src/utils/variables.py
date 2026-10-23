import logging
from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)

# Precipitation values below this threshold are numerical noise and are set to zero
NEGLIGIBLE_THRESHOLD = 1e-15

# Physically plausible range for 2m temperature, in Kelvin (record lows/highs are ~184 K and ~330 K)
TEMPERATURE_RANGE_K = (150.0, 350.0)

KELVIN_OFFSET = 273.15


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


def clean_temperature_values(df, column="t2m"):
    """
    Replace physically implausible 2m temperature values with NaN.

    Args:
        df (pd.DataFrame): DataFrame containing the temperature column, in Kelvin.
        column (str): Name of the temperature column.

    Returns:
        pd.DataFrame: The same DataFrame with implausible values masked.
    """
    low, high = TEMPERATURE_RANGE_K
    implausible = (df[column] < low) | (df[column] > high)
    if implausible.any():
        logger.warning("Masking %d implausible temperature values outside %s-%s K", implausible.sum(), low, high)
        df[column] = df[column].mask(implausible)
    return df


@dataclass(frozen=True)
class VariableSpec:
    """
    Variable-specific logic of an ERA5 single level variable.

    Attributes:
        name: Folder name of the variable in the bucket, also used as dataset name.
        short_name: Name of the variable inside the NetCDF file (and of the Parquet column).
        source_unit: Unit stored in the Parquet files, as provided by ERA5.
        report_unit: Unit used when reporting aggregated values.
        aggregations: Aggregations that are meaningful for the variable.
        clean: Cleaning applied to the extracted DataFrame.
        to_report_unit: Conversion from source_unit to report_unit.
    """

    name: str
    short_name: str
    source_unit: str
    report_unit: str
    aggregations: tuple[str, ...]
    clean: Callable[[pd.DataFrame, str], pd.DataFrame]
    to_report_unit: Callable[[float], float]


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
