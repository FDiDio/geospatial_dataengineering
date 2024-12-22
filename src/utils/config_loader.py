import yaml

def load_configuration(config_path):
    """
    Load configuration settings from the specified YAML file.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Loaded configuration dictionary.
    """
    print(f"Loading configuration from {config_path}")
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)