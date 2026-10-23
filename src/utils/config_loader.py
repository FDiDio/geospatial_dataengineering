import logging
import os
import yaml
from utils.data_writer import dataset_path
from utils.variables import get_variable

logger = logging.getLogger(__name__)

# Configuration keys holding local filesystem paths
PATH_KEYS = ("output_path", "query_output_path", "h3_lookup_path")

REQUIRED_KEYS = ("bucket_path", "output_path", "query_output_path", "h3_lookup_path",
                 "h3_resolution", "variables", "file_name")


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
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file) or {}

    missing = [key for key in REQUIRED_KEYS if key not in config]
    if missing:
        raise KeyError(f"Missing configuration keys in {config_path}: {', '.join(missing)}")

    config_dir = os.path.dirname(os.path.abspath(config_path))
    for key in PATH_KEYS:
        config[key] = os.path.normpath(os.path.join(config_dir, config[key]))

    for name in config["variables"]:
        get_variable(name)
        # Query results inside a dataset directory would be read back as data
        if is_within(config["query_output_path"], dataset_path(config["output_path"], name)):
            raise ValueError(f"query_output_path must not be inside the {name} dataset directory")

    config.setdefault("max_retries", 3)
    config.setdefault("queries", [])
    return config


def is_within(path, directory):
    """Return True if path is the directory itself or one of its descendants."""
    path, directory = os.path.abspath(path), os.path.abspath(directory)
    return path == directory or path.startswith(directory + os.sep)
