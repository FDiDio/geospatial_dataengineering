import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.config_loader import load_configuration
from utils.data_extractor import generate_date_range
from utils.data_writer import SINKS
from utils.pipeline import Pipeline
from utils.query_runner import run_queries_on_data
from utils.sources import SOURCES
from utils.variables import VARIABLES, get_variable

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")


def positive_int(text):
    """An argparse type: an integer of at least 1, as a number of workers."""
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid int value: {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, not {value}")
    return value


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="Process ERA5 data from GCS.")
    parser.add_argument(
        "--start_date", type=str, default="20220101", help="Start date, YYYYMMDD or today-N (default: 20220101)"
    )
    parser.add_argument(
        "--end_date", type=str, default="20221231", help="End date, YYYYMMDD or today-N (default: 20221231)"
    )
    parser.add_argument(
        "--config",
        "--config_file",
        dest="config",
        type=str,
        default=os.path.normpath(DEFAULT_CONFIG),
        help="Path to the configuration YAML file (default: config/config.yaml in the project)",
    )
    parser.add_argument(
        "--variables",
        nargs="+",
        choices=list(VARIABLES),
        help="Variables to process (default: the variables listed in the configuration file)",
    )
    parser.add_argument(
        "--workers",
        type=positive_int,
        help="Variable-days processed in parallel (default: 'workers' of the configuration)",
    )
    parser.add_argument("--source", choices=SOURCES, help="Read the daily NetCDF files or the Zarr store")
    parser.add_argument("--sink", choices=SINKS, help="Write Hive-partitioned Parquet files or Iceberg tables")
    parser.add_argument("--overwrite", action="store_true", help="Reprocess days that have already been written")
    parser.add_argument("--skip_queries", action="store_true", help="Do not run the sample queries at the end")
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args(argv)


def process_dates(pipeline, date_range, variable_names, overwrite=False, workers=1):
    """
    Process every variable of every day of the range, skipping what is already written unless overwrite is set.

    Variable-days are processed by a pool of worker threads: downloads, decoding and
    Parquet encoding release the GIL, so the workers overlap network and CPU work.
    A failing variable or day is logged and does not stop the remaining ones.

    Returns:
        list: (variable name, (year, month, day)) pairs that could not be processed.
    """
    variables = [get_variable(name) for name in variable_names]
    tasks = []
    for year, month, day in date_range:
        for variable in variables:
            if not overwrite and pipeline.sink.exists(variable.name, year, month, day):
                logger.info("Skipping %s %d-%02d-%02d, already processed", variable.name, year, month, day)
                continue
            tasks.append((variable, (year, month, day)))

    logger.info("Processing %d variable-days with %d worker(s)", len(tasks), workers)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(pipeline.process, variable, *date): (variable.name, date) for variable, date in tasks}
        for future in as_completed(futures):
            error = future.exception()
            if error is not None:
                name, (year, month, day) = futures[future]
                logger.error("Failed to process %s %d-%02d-%02d", name, year, month, day, exc_info=error)

    return [task for future, task in futures.items() if future.exception() is not None]


def main(argv=None):
    args = parse_arguments(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # Load configuration from YAML, the command line arguments take precedence
    config = load_configuration(args.config)
    for key in ("workers", "source", "sink"):
        if getattr(args, key) is not None:
            config[key] = getattr(args, key)
    variable_names = args.variables or config["variables"]

    # Generate date range based on the provided start and end dates
    date_range = generate_date_range(args.start_date, args.end_date)

    # Process the data of each variable for each date in the range
    pipeline = Pipeline.from_config(config)
    start = time.perf_counter()
    failed = process_dates(pipeline, date_range, variable_names, overwrite=args.overwrite, workers=config["workers"])
    logger.info("Processing finished in %.1fs", time.perf_counter() - start)
    if failed:
        logger.error(
            "%d of %d variable-days failed: %s",
            len(failed),
            len(date_range) * len(variable_names),
            ", ".join(f"{name} {y}-{m:02d}-{d:02d}" for name, (y, m, d) in failed),
        )

    # After processing, run sample queries
    if not args.skip_queries:
        run_queries_on_data(pipeline.sink, config["query_output_path"], config["queries"])

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
