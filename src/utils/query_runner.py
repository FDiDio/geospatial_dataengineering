import logging

import pandas as pd

# PARTITIONING is re-exported for reading the Parquet datasets directly with PyArrow
from utils.data_writer import PARTITIONING, Sink, save_query_results_to_parquet
from utils.filters import Condition, to_arrow_expression
from utils.variables import get_variable

__all__ = ["PARTITIONING", "build_filter_expression", "query_conditions", "run_queries_on_data"]

logger = logging.getLogger(__name__)

QUERY_KEYS = {"name", "variable", "start", "end", "h3_index", "bbox", "point", "aggregate"}


def query_conditions(query) -> list[Condition]:
    """
    Translate the filters of a query definition of the configuration file into storage independent conditions.

    Args:
        query (dict): Query definition (see the 'queries' section of config.yaml).

    Returns:
        list: (column, operator, value) conditions, all of which must hold.
    """
    unknown = set(query) - QUERY_KEYS
    if unknown:
        raise ValueError(f"Unknown keys in query '{query.get('name')}': {', '.join(sorted(unknown))}")

    conditions: list[Condition] = []
    if "start" in query:
        start = pd.Timestamp(query["start"])
        conditions.append(("time", ">=", start.to_pydatetime()))
    if "end" in query:
        # The end day is included: keep every hour before midnight of the following day
        end = pd.Timestamp(query["end"]) + pd.Timedelta(days=1)
        conditions.append(("time", "<", end.to_pydatetime()))
    if "h3_index" in query:
        conditions.append(("h3_index", "==", query["h3_index"]))
    if "bbox" in query:
        bbox = query["bbox"]
        conditions += [
            ("latitude", ">=", bbox["lat_min"]),
            ("latitude", "<=", bbox["lat_max"]),
            ("longitude", ">=", bbox["lon_min"]),
            ("longitude", "<=", bbox["lon_max"]),
        ]
    if "point" in query:
        # Grid coordinates are floats: match the nearest grid point within a tolerance
        point = query["point"]
        tolerance = point.get("tolerance", 0.01)
        conditions += [
            ("latitude", ">=", point["latitude"] - tolerance),
            ("latitude", "<=", point["latitude"] + tolerance),
            ("longitude", ">=", point["longitude"] - tolerance),
            ("longitude", "<=", point["longitude"] + tolerance),
        ]

    return conditions


def build_filter_expression(query):
    """
    Build a PyArrow filter expression from a query definition of the configuration file.

    Args:
        query (dict): Query definition (see the 'queries' section of config.yaml).

    Returns:
        pyarrow.dataset.Expression: Filter expression, or None if the query has no filters.
    """
    return to_arrow_expression(query_conditions(query))


def query_aggregations(query, variable):
    """
    Return the aggregations requested by the query, checking they make sense for its variable.

    Args:
        query (dict): Query definition.
        variable (VariableSpec): Variable targeted by the query.

    Returns:
        list: Requested aggregations, empty if the query returns rows.
    """
    aggregate = query.get("aggregate", [])
    aggregations = [aggregate] if isinstance(aggregate, str) else list(aggregate)
    unsupported = [agg for agg in aggregations if agg not in variable.aggregations]
    if unsupported:
        raise ValueError(
            f"Unsupported aggregate {', '.join(unsupported)} in query '{query['name']}' "
            f"for {variable.name}, supported: {', '.join(variable.aggregations)}"
        )
    return aggregations


def run_query(sink: Sink, query, variable, output_query_dir):
    """
    Run a single query on the data of its variable and save or report its result.

    Args:
        sink (Sink): Storage of the processed data, see utils.data_writer.
        query (dict): Query definition.
        variable (VariableSpec): Variable targeted by the query.
        output_query_dir (str): Directory to save the query results.

    Returns:
        pd.DataFrame | dict | None: Filtered rows, or the aggregated values in the report unit
        of the variable; None if the variable has not been processed yet.
    """
    name = query["name"]
    aggregations = query_aggregations(query, variable)
    columns = [variable.short_name] if aggregations else None
    table = sink.read(variable.name, query_conditions(query), columns)
    if table is None:
        logger.warning("Skipping query '%s': no processed %s data", name, variable.name)
        return None

    df = table.to_pandas()
    if df.empty:
        logger.warning("Query '%s' returned no rows, check its filters against the processed dates", name)

    if aggregations:
        values = df[variable.short_name]
        results = {agg: variable.to_report_unit(float(getattr(values, agg)())) for agg in aggregations}
        report = ", ".join(f"{agg} {value:.2f} {variable.report_unit}" for agg, value in results.items())
        logger.info("Query '%s': %s %s over %d rows", name, variable.name, report, len(df))
        return results

    logger.info("Query '%s': %d rows\n%s", name, len(df), df.head())
    save_query_results_to_parquet(df, name, output_query_dir)
    return df


def run_queries_on_data(sink: Sink, output_query_dir, queries):
    """
    Run the configured queries on the processed data.

    Queries on a variable that has not been processed yet are skipped with a warning.

    Args:
        sink (Sink): Storage of the processed data (Parquet datasets or Iceberg tables).
        output_query_dir (str): Directory to save the query results.
        queries (list): Query definitions from the configuration file.

    Returns:
        dict: Query name mapped to its filtered DataFrame or aggregated values.
    """
    logger.info("Running %d queries on the %s", len(queries), type(sink).__name__)
    results = {}

    for query in queries:
        if "variable" not in query:
            raise ValueError(f"Query '{query.get('name')}' does not specify its variable")
        variable = get_variable(query["variable"])

        result = run_query(sink, query, variable, output_query_dir)
        if result is not None:
            results[query["name"]] = result

    return results
