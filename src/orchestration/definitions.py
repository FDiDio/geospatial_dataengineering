"""
Dagster definitions of the ERA5 lakehouse: uv run dagster dev -m orchestration.definitions -d src

- one daily partitioned asset per ERA5 variable (ingestion, gated by the data contract)
- the dbt models, partitioned by day too, with the dbt tests as asset checks
- a sensor launching each day once ERA5 publishes it
"""

import dagster as dg
from dagster_dbt import DbtCliResource

from orchestration.assets import build_dbt_assets, build_ingestion_asset, daily_partitions, dbt_project, h3_lookup
from orchestration.resources import Era5PipelineResource, config_path
from orchestration.sensors import build_availability_sensor
from utils.config_loader import load_configuration

config = load_configuration(config_path())
partitions = daily_partitions(config["partitions_start_date"])

ingestion_assets = [build_ingestion_asset(name, partitions, config["sink"]) for name in config["variables"]]
dbt_models = build_dbt_assets(partitions)

era5_daily_job = dg.define_asset_job(
    "era5_daily",
    selection=dg.AssetSelection.assets(*ingestion_assets, dbt_models),
    partitions_def=partitions,
    description="Ingest the ERA5 variables of a day, then build the dbt models of that day.",
)

defs = dg.Definitions(
    assets=[*ingestion_assets, h3_lookup, dbt_models],
    jobs=[era5_daily_job],
    sensors=[build_availability_sensor(era5_daily_job.name, config["variables"], partitions)],
    resources={
        "era5": Era5PipelineResource(config_path=config_path()),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
