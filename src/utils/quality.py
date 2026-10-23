import logging

import pandas as pd
from pandera.errors import SchemaErrors
from pandera.pandas import Check, Column, DataFrameSchema

from utils.variables import VariableSpec

logger = logging.getLogger(__name__)

# Share of missing values above which a variable-day is rejected (values are masked when implausible)
DEFAULT_MAX_NULL_FRACTION = 0.01

HOURS_PER_DAY = 24


class DataContractError(ValueError):
    """A variable-day does not satisfy its data contract and must not be written."""


def well_formed_h3_cells(resolution: int) -> Check:
    """Check that every distinct value is an H3 cell identifier at the given resolution."""
    # Cell identifiers are 15 hex digits: mode 1 ("8"), then the resolution
    pattern = rf"^8{resolution:x}[0-9a-f]{{13}}$"
    return Check(
        lambda cells: pd.Series(cells.unique()).str.fullmatch(pattern).all(),
        error=f"H3 cell identifiers at resolution {resolution}",
    )


def complete_hourly_grid(df: pd.DataFrame) -> bool:
    """Every grid point has a value for each of the 24 hours of the day."""
    times, latitudes, longitudes = (df[c].nunique() for c in ("time", "latitude", "longitude"))
    return times == HOURS_PER_DAY and len(df) == times * latitudes * longitudes


def variable_schema(
    variable: VariableSpec,
    day: pd.Timestamp,
    h3_resolution: int,
    max_null_fraction: float = DEFAULT_MAX_NULL_FRACTION,
) -> DataFrameSchema:
    """
    Build the data contract of a variable-day, the gate before it is written.

    Args:
        variable (VariableSpec): Variable of the data.
        day (pd.Timestamp): Day of the data, at midnight.
        h3_resolution (int): Resolution of the H3 index.
        max_null_fraction (float): Maximum share of missing values of the variable.

    Returns:
        DataFrameSchema: Pandera schema of the DataFrame written to the sink.
    """
    low, high = variable.valid_range
    column = variable.short_name
    return DataFrameSchema(
        {
            "time": Column(
                checks=[
                    Check(pd.api.types.is_datetime64_any_dtype, error="datetime dtype"),
                    Check.in_range(day, day + pd.Timedelta(days=1), include_max=False),
                ]
            ),
            "latitude": Column("float32", Check.in_range(-90, 90)),
            "longitude": Column("float32", Check.in_range(0, 360, include_max=False)),
            column: Column("float32", Check.in_range(low, high), nullable=True),
            "h3_index": Column(checks=well_formed_h3_cells(h3_resolution)),
        },
        checks=[
            Check(complete_hourly_grid, error="complete hourly grid: 24 hours x all the grid points"),
            Check(
                lambda df: df[column].isna().mean() <= max_null_fraction,
                error=f"at most {max_null_fraction:.1%} missing {column} values",
            ),
        ],
        strict=True,
        ordered=False,
    )


def validate_variable_day(
    df: pd.DataFrame,
    variable: VariableSpec,
    year: int,
    month: int,
    day: int,
    h3_resolution: int,
    max_null_fraction: float = DEFAULT_MAX_NULL_FRACTION,
) -> pd.DataFrame:
    """
    Validate a variable-day against its data contract, collecting every violation.

    Raises:
        DataContractError: With a summary of the failed checks.
    """
    schema = variable_schema(variable, pd.Timestamp(year=year, month=month, day=day), h3_resolution, max_null_fraction)
    try:
        return schema.validate(df, lazy=True, inplace=True)
    except SchemaErrors as errors:
        failures = errors.failure_cases.groupby(["column", "check"], dropna=False).size()
        summary = "; ".join(
            f"{col if pd.notna(col) else 'dataframe'}: {check} ({n} failures)" for (col, check), n in failures.items()
        )
        raise DataContractError(
            f"{variable.name} {year}-{month:02d}-{day:02d} violates its data contract: {summary}"
        ) from None
