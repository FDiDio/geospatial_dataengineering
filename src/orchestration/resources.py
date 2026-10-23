import os
from contextlib import contextmanager
from typing import Any

import dagster as dg
from pydantic import PrivateAttr

from utils.config_loader import load_configuration
from utils.pipeline import Pipeline

PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_CONFIG = os.path.join(PROJECT_ROOT, "config", "config.yaml")


def config_path() -> str:
    """Configuration file of the pipeline: ERA5_CONFIG, or the one of the project."""
    return os.environ.get("ERA5_CONFIG", DEFAULT_CONFIG)


class Era5PipelineResource(dg.ConfigurableResource):
    """The ingestion pipeline built from its YAML configuration, shared by the assets of a run."""

    config_path: str = DEFAULT_CONFIG

    _pipeline: Pipeline | None = PrivateAttr(default=None)

    @property
    def config(self) -> dict[str, Any]:
        return load_configuration(self.config_path)

    def pipeline(self) -> Pipeline:
        if self._pipeline is None:
            self._pipeline = Pipeline.from_config(self.config)
        return self._pipeline

    def dbt_env(self) -> dict[str, str]:
        """Environment of the dbt project, pointing it to the data written by the pipeline."""
        config = self.config
        return {
            "ERA5_FORMAT": config["sink"],
            "ERA5_LAKE_PATH": config["output_path"].replace(os.sep, "/"),
            "ERA5_ICEBERG_WAREHOUSE": config["iceberg_warehouse_path"].replace(os.sep, "/"),
            "ERA5_ICEBERG_NAMESPACE": config["iceberg_namespace"],
            "ERA5_H3_LOOKUP_PATH": config["h3_lookup_path"].replace(os.sep, "/"),
            "ERA5_H3_RESOLUTION": str(config["h3_resolution"]),
            "DUCKDB_PATH": os.environ.get("DUCKDB_PATH", os.path.join(config["output_path"], "warehouse.duckdb")),
        }


@contextmanager
def environment(variables: dict[str, str]):
    """Temporarily set environment variables, inherited by the subprocesses started meanwhile."""
    previous = {key: os.environ.get(key) for key in variables}
    os.environ.update(variables)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
