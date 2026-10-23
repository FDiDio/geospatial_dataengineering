import argparse
import logging
import os
import sys
from utils.config_loader import load_configuration
from utils.data_extractor import extract_variable_data, generate_date_range
from utils.h3_processor import enrich_with_h3
from utils.data_writer import save_data_to_parquet, daily_output_path
from utils.query_runner import run_queries_on_data
from utils.variables import VARIABLES, get_variable

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description="Process ERA5 data from GCS.")
    parser.add_argument('--start_date', type=str, default='20220101', help='Start date in YYYYMMDD format (default: 20220101)')
    parser.add_argument('--end_date', type=str, default='20221231', help='End date in YYYYMMDD format (default: 20221231)')
    parser.add_argument('--config', '--config_file', dest='config', type=str, default=os.path.normpath(DEFAULT_CONFIG),
                        help='Path to the configuration YAML file (default: config/config.yaml in the project)')
    parser.add_argument('--variables', nargs='+', choices=list(VARIABLES),
                        help='Variables to process (default: the variables listed in the configuration file)')
    parser.add_argument('--overwrite', action='store_true', help='Reprocess days whose Parquet file already exists')
    parser.add_argument('--skip_queries', action='store_true', help='Do not run the sample queries at the end')
    parser.add_argument('--log_level', type=str, default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Logging verbosity (default: INFO)')
    return parser.parse_args(argv)


def process_dates(config, date_range, variable_names, overwrite=False):
    """
    Process every variable of every day of the range, skipping what is already written unless overwrite is set.

    A failing variable or day is logged and does not stop the remaining ones.

    Returns:
        list: (variable name, (year, month, day)) pairs that could not be processed.
    """
    failed = []
    h3_lookup = None
    variables = [get_variable(name) for name in variable_names]

    for year, month, day in date_range:
        for variable in variables:
            if not overwrite and os.path.exists(daily_output_path(config['output_path'], variable.name, year, month, day)):
                logger.info("Skipping %s %d-%02d-%02d, already processed", variable.name, year, month, day)
                continue

            try:
                file_name = config["file_name"].format(year=year, month=month, day=day)  # Format the file name using date
                df = extract_variable_data(config['bucket_path'], year, month, day, variable,
                                           file_name, max_retries=config['max_retries'])
                # All the variables share the same grid, hence the same lookup table
                df_with_h3, h3_lookup = enrich_with_h3(df, config['h3_resolution'], config['h3_lookup_path'], h3_lookup)
                save_data_to_parquet(df_with_h3, config['output_path'], variable.name, year, month, day)
            except Exception:
                logger.exception("Failed to process %s %d-%02d-%02d", variable.name, year, month, day)
                failed.append((variable.name, (year, month, day)))

    return failed


def main(argv=None):
    args = parse_arguments(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    # Load configuration from YAML
    config = load_configuration(args.config)
    variable_names = args.variables or config['variables']

    # Generate date range based on the provided start and end dates
    date_range = generate_date_range(args.start_date, args.end_date)

    # Process the data of each variable for each date in the range
    failed = process_dates(config, date_range, variable_names, overwrite=args.overwrite)
    if failed:
        logger.error("%d of %d variable-days failed: %s", len(failed), len(date_range) * len(variable_names),
                     ", ".join(f"{name} {y}-{m:02d}-{d:02d}" for name, (y, m, d) in failed))

    # After processing, run sample queries
    if not args.skip_queries:
        run_queries_on_data(config['output_path'], config['query_output_path'], config['queries'])

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
