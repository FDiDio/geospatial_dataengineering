import datetime as dt
import logging
import os
import threading
import time
import warnings
from collections.abc import Sequence
from functools import reduce
from typing import Any

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError, TableAlreadyExistsError
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
)
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from utils.filters import Condition

logger = logging.getLogger(__name__)

# The first write of a day replaces nothing: pyiceberg warns about it on every new partition
warnings.filterwarnings("ignore", message="Delete operation did not match any records", category=UserWarning)

ICEBERG_OPERATORS = {
    ">=": GreaterThanOrEqual,
    ">": GreaterThan,
    "<=": LessThanOrEqual,
    "<": LessThan,
    "==": EqualTo,
}


def to_iceberg_expression(conditions: Sequence[Condition]) -> BooleanExpression:
    """Combine the conditions into an Iceberg row filter, used to prune partitions and files."""
    expressions = [
        ICEBERG_OPERATORS[op](column, value.isoformat() if isinstance(value, dt.datetime) else value)
        for column, op, value in conditions
    ]
    return reduce(And, expressions) if expressions else AlwaysTrue()


def day_filter(year: int, month: int, day: int) -> BooleanExpression:
    """Rows of a single day, i.e. exactly one partition of the tables."""
    start = dt.datetime(year, month, day)
    return to_iceberg_expression([("time", ">=", start), ("time", "<", start + dt.timedelta(days=1))])


def local_catalog_properties(warehouse_path: str) -> dict[str, str]:
    """Properties of a local SQL catalog: a SQLite database next to the table files."""
    os.makedirs(warehouse_path, exist_ok=True)
    warehouse = os.path.abspath(warehouse_path).replace(os.sep, "/")
    return {"type": "sql", "uri": f"sqlite:///{warehouse}/catalog.db", "warehouse": warehouse}


class IcebergSink:
    """
    Apache Iceberg tables, one per variable (<namespace>.<variable>), partitioned by day.

    Writing a day replaces its partition in a single atomic commit: reprocessing is
    idempotent and readers never see a partial day. Commits are serialized within the
    process and retried when another process committed to the same table meanwhile.
    """

    def __init__(self, catalog_properties: dict[str, Any], namespace: str = "era5", commit_retries: int = 5):
        self.catalog = load_catalog(namespace, **catalog_properties)
        self.namespace = namespace
        self.commit_retries = commit_retries
        self.catalog.create_namespace_if_not_exists(namespace)
        self._lock = threading.Lock()
        self._days: dict[str, set[dt.date]] = {}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "IcebergSink":
        """Use the 'iceberg_catalog' properties if set, otherwise a local catalog in 'iceberg_warehouse_path'."""
        properties = config.get("iceberg_catalog") or local_catalog_properties(config["iceberg_warehouse_path"])
        return cls(properties, config["iceberg_namespace"])

    def identifier(self, variable_name: str) -> str:
        return f"{self.namespace}.{variable_name}"

    def load_table(self, variable_name: str) -> Table | None:
        try:
            return self.catalog.load_table(self.identifier(variable_name))
        except NoSuchTableError:
            return None

    def exists(self, variable_name: str, year: int, month: int, day: int) -> bool:
        with self._lock:
            if variable_name not in self._days:
                table = self.load_table(variable_name)
                partitions = [] if table is None else table.inspect.partitions().to_pylist()
                self._days[variable_name] = {row["partition"]["day"] for row in partitions}
            return dt.date(year, month, day) in self._days[variable_name]

    def write(self, df: pd.DataFrame, variable_name: str, year: int, month: int, day: int) -> None:
        logger.info("Writing %s data for %d-%02d-%02d to Iceberg", variable_name, year, month, day)
        data = pa.Table.from_pandas(df, preserve_index=False)
        # Iceberg v2 timestamps have microsecond precision
        data = data.set_column(data.schema.get_field_index("time"), "time", data["time"].cast(pa.timestamp("us")))

        with self._lock:
            table = self.load_table(variable_name) or self._create_table(variable_name, data.schema)
            for attempt in range(1, self.commit_retries + 1):
                try:
                    schema = table.schema().as_arrow()
                    table.overwrite(
                        data.select(schema.names).cast(schema), overwrite_filter=day_filter(year, month, day)
                    )
                    break
                except CommitFailedException:
                    if attempt == self.commit_retries:
                        raise
                    logger.warning(
                        "Concurrent commit on %s, retrying (%d/%d)", table.name(), attempt, self.commit_retries
                    )
                    time.sleep(0.5 * attempt)
                    table.refresh()
            if variable_name in self._days:
                self._days[variable_name].add(dt.date(year, month, day))

    def read(
        self, variable_name: str, conditions: Sequence[Condition], columns: list[str] | None = None
    ) -> pa.Table | None:
        table = self.load_table(variable_name)
        if table is None:
            return None
        selected = tuple(columns) if columns else ("*",)
        return table.scan(row_filter=to_iceberg_expression(conditions), selected_fields=selected).to_arrow()

    def _create_table(self, variable_name: str, schema: pa.Schema) -> Table:
        identifier = self.identifier(variable_name)
        logger.info("Creating Iceberg table %s partitioned by day", identifier)
        try:
            with (
                self.catalog.create_table_transaction(identifier, schema=schema) as transaction,
                transaction.update_spec() as spec,
            ):
                spec.add_field("time", DayTransform(), "day")
        except TableAlreadyExistsError:
            pass  # created meanwhile by another process
        return self.catalog.load_table(identifier)
