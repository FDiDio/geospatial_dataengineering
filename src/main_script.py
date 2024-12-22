import argparse
from utils.config_loader import load_configuration
from utils.data_extractor import extract_precipitation_data, generate_date_range
from utils.h3_processor import precompute_h3_index_lookup, map_h3_indices_to_dataframe
from utils.data_writer import save_data_to_parquet
from utils.query_runner import run_queries_on_data


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process precipitation data from GCS.")
    parser.add_argument('--start_date', type=str, default='20220101', help='Start date in YYYYMMDD format (default: 20220101)')
    parser.add_argument('--end_date', type=str, default='20221231', help='End date in YYYYMMDD format (default: 20221231)')
    parser.add_argument('--config_file', type=str, default='../config/config.yaml', help='Path to the configuration YAML file')
    args = parser.parse_args()

    # Load configuration from YAML
    config = load_configuration(args.config_file)

    # Use command-line arguments for the date range
    start_date = args.start_date
    end_date = args.end_date

    # Generate date range based on the provided start and end dates
    date_range = generate_date_range(start_date, end_date)
    
    # Process precipitation data for each date in the range
    for year, month, day in date_range:
        variable_name = config["variable_name"]  # Read from config
        file_name = config["file_name"].format(year=year, month=month, day=day)  # Format the file name using date
        df = extract_precipitation_data(config['bucket_path'], year, month, day, variable_name, file_name)
        h3_lookup = precompute_h3_index_lookup(df, config['h3_resolution'])
        df_with_h3 = map_h3_indices_to_dataframe(df, h3_lookup)
        save_data_to_parquet(df_with_h3, config['output_path'], year, month, day)

    # After processing, run sample queries
    run_queries_on_data(config['output_path'], config['query_output_path'])

if __name__ == "__main__":
    main()
