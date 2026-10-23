import logging
import operator
import os
from functools import reduce

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from utils.data_writer import dataset_path, save_query_results_to_parquet
from utils.variables import get_variable

logger = logging.getLogger(__name__)

# Hive-style partitioning written by data_writer.daily_output_path
PARTITIONING = ds.partitioning(pa.schema([("year", pa.int16()), ("month", pa.int8())]), flavor="hive")

QUERY_KEYS = {"name", "variable", "start", "end", "h3_index", "bbox", "point", "aggregate"}


def build_filter_expression(query):
    """
    Build a PyArrow filter expression from a query definition of the configuration file.

    Args:
        query (dict): Query definition (see the 'queries' section of config.yaml).

    Returns:
        pyarrow.dataset.Expression: Filter expression, or None if the query has no filters.
    """
    unknown = set(query) - QUERY_KEYS
    if unknown:
        raise ValueError(f"Unknown keys in query '{query.get('name')}': {', '.join(sorted(unknown))}")

    conditions = []
    if "start" in query:
        start = pd.Timestamp(query["start"])
        conditions.append(ds.field("time") >= start.to_pydatetime())
    if "end" in query:
        # The end day is included: keep every hour before midnight of the following day
        end = pd.Timestamp(query["end"]) + pd.Timedelta(days=1)
        conditions.append(ds.field("time") < end.to_pydatetime())
    if "h3_index" in query:
        conditions.append(ds.field("h3_index") == query["h3_index"])
    if "bbox" in query:
        bbox = query["bbox"]
        conditions += [
            ds.field("latitude") >= bbox["lat_min"],
            ds.field("latitude") <= bbox["lat_max"],
            ds.field("longitude") >= bbox["lon_min"],
            ds.field("longitude") <= bbox["lon_max"],
        ]
    if "point" in query:
        # Grid coordinates are floats: match the nearest grid point within a tolerance
        point = query["point"]
        tolerance = point.get("tolerance", 0.01)
        conditions += [
            ds.field("latitude") >= point["latitude"] - tolerance,
            ds.field("latitude") <= point["latitude"] + tolerance,
            ds.field("longitude") >= point["longitude"] - tolerance,
            ds.field("longitude") <= point["longitude"] + tolerance,
        ]

    return reduce(operator.and_, conditions) if conditions else None


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


def run_query(dataset, query, variable, output_query_dir):
    """
    Run a single query on the dataset of its variable and save or report its result.

    Args:
        dataset (pyarrow.dataset.Dataset): Dataset of the variable.
        query (dict): Query definition.
        variable (VariableSpec): Variable targeted by the query.
        output_query_dir (str): Directory to save the query results.

    Returns:
        pd.DataFrame | dict: Filtered rows, or the aggregated values in the report unit of the variable.
    """
    name = query["name"]
    aggregations = query_aggregations(query, variable)
    filter_expr = build_filter_expression(query)
    columns = [variable.short_name] if aggregations else None
    df = dataset.to_table(filter=filter_expr, columns=columns).to_pandas()

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


def run_queries_on_data(output_path, output_query_dir, queries):
    """
    Run the configured queries on the Parquet datasets using PyArrow.

    Queries on a variable that has not been processed yet are skipped with a warning.

    Args:
        output_path (str): Root output directory, containing one partitioned dataset per variable.
        output_query_dir (str): Directory to save the query results.
        queries (list): Query definitions from the configuration file.

    Returns:
        dict: Query name mapped to its filtered DataFrame or aggregated values.
    """
    logger.info("Running %d queries on the Parquet files using PyArrow", len(queries))
    datasets = {}
    results = {}

    for query in queries:
        if "variable" not in query:
            raise ValueError(f"Query '{query.get('name')}' does not specify its variable")
        variable = get_variable(query["variable"])

        if variable.name not in datasets:
            directory = dataset_path(output_path, variable.name)
            datasets[variable.name] = (
                ds.dataset(directory, format="parquet", partitioning=PARTITIONING) if os.path.isdir(directory) else None
            )
        if datasets[variable.name] is None:
            logger.warning("Skipping query '%s': no processed %s data", query["name"], variable.name)
            continue

        results[query["name"]] = run_query(datasets[variable.name], query, variable, output_query_dir)

    return results
