import copy
import logging
import os

import yaml

from utils.data_writer import SINKS, dataset_path
from utils.quality import DEFAULT_MAX_NULL_FRACTION
from utils.sources import SOURCES
from utils.variables import get_variable

logger = logging.getLogger(__name__)

# Configuration keys holding local filesystem paths
PATH_KEYS = ("output_path", "query_output_path", "h3_lookup_path")
OPTIONAL_PATH_KEYS = ("iceberg_warehouse_path",)

REQUIRED_KEYS = (
    "bucket_path",
    "output_path",
    "query_output_path",
    "h3_lookup_path",
    "h3_resolution",
    "variables",
    "file_name",
)

DEFAULTS = {
    "max_retries": 3,
    "queries": [],
    "workers": 1,
    "source": "netcdf",
    "zarr_store": "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    # The ERA5 bucket is public: anonymous access avoids looking up Google credentials
    "storage_options": {"token": "anon"},
    "sink": "parquet",
    "iceberg_namespace": "era5",
    "iceberg_catalog": None,
    "max_null_fraction": DEFAULT_MAX_NULL_FRACTION,
}


def load_configuration(config_path):
    """
    Load configuration settings from the specified YAML file.

    Relative local paths are resolved against the directory containing the
    configuration file, so the result does not depend on the working directory.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Loaded configuration dictionary.
    """
    logger.info("Loading configuration from %s", config_path)
    with open(config_path) as file:
        config = yaml.safe_load(file) or {}

    missing = [key for key in REQUIRED_KEYS if key not in config]
    if missing:
        raise KeyError(f"Missing configuration keys in {config_path}: {', '.join(missing)}")

    config_dir = os.path.dirname(os.path.abspath(config_path))
    for key in PATH_KEYS + OPTIONAL_PATH_KEYS:
        if key in config:
            config[key] = os.path.normpath(os.path.join(config_dir, config[key]))
    config.setdefault("iceberg_warehouse_path", os.path.join(config["output_path"], "iceberg"))

    for name in config["variables"]:
        get_variable(name)
        # Query results inside a dataset directory would be read back as data
        if is_within(config["query_output_path"], dataset_path(config["output_path"], name)):
            raise ValueError(f"query_output_path must not be inside the {name} dataset directory")

    for key, value in DEFAULTS.items():
        config.setdefault(key, copy.deepcopy(value))
    check_choice(config, "source", SOURCES)
    check_choice(config, "sink", SINKS)
    if not isinstance(config["workers"], int) or config["workers"] < 1:
        raise ValueError(f"workers must be a positive integer, got {config['workers']!r}")
    return config


def check_choice(config, key, choices):
    """Raise ValueError if the configuration value is not one of the choices."""
    if config[key] not in choices:
        raise ValueError(f"Unsupported {key} '{config[key]}', supported: {', '.join(choices)}")


def is_within(path, directory):
    """Return True if path is the directory itself or one of its descendants."""
    path, directory = os.path.abspath(path), os.path.abspath(directory)
    return path == directory or path.startswith(directory + os.sep)
