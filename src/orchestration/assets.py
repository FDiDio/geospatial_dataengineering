import json
import os
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from orchestration.resources import PROJECT_ROOT, Era5PipelineResource, environment
from utils.quality import DataContractError
from utils.variables import get_variable

INGESTION_GROUP = "ingestion"
INGESTION_POOL = "era5_ingestion"

dbt_project = DbtProject(project_dir=os.path.join(PROJECT_ROOT, "transform"))
# Under `dagster dev`, parse the dbt project at load time so the manifest is always current
dbt_project.prepare_if_dev()


def daily_partitions(start_date: str) -> dg.DailyPartitionsDefinition:
    return dg.DailyPartitionsDefinition(start_date=start_date)


def build_ingestion_asset(
    variable_name: str, partitions_def: dg.PartitionsDefinition, sink: str = "parquet"
) -> dg.AssetsDefinition:
    """One daily partitioned asset per ERA5 variable, the key matching the dbt source table (era5/<variable>)."""
    variable = get_variable(variable_name)
    key = dg.AssetKey(["era5", variable.name])

    @dg.asset(
        key=key,
        partitions_def=partitions_def,
        group_name=INGESTION_GROUP,
        kinds={"python", sink},
        pool=INGESTION_POOL,
        retry_policy=dg.RetryPolicy(max_retries=2, delay=60, backoff=dg.Backoff.EXPONENTIAL),
        check_specs=[dg.AssetCheckSpec("data_contract", asset=key, blocking=True)],
        description=(
            f"Hourly {variable.name} ({variable.short_name}, {variable.source_unit}) on the ERA5 0.25° grid "
            "indexed with H3, one partition per day. Written only if it satisfies its data contract."
        ),
        metadata={"column": variable.short_name, "unit": variable.source_unit},
    )
    def ingestion_asset(context: dg.AssetExecutionContext, era5: Era5PipelineResource) -> dg.MaterializeResult:
        day = context.partition_time_window.start
        try:
            result = era5.pipeline().process(variable, day.year, day.month, day.day)
        except DataContractError as error:
            raise dg.Failure(description=str(error), allow_retries=False) from None

        return dg.MaterializeResult(
            metadata={
                "rows": result.rows,
                "missing_fraction": dg.MetadataValue.float(result.missing_fraction),
                "min": dg.MetadataValue.float(result.minimum),
                "max": dg.MetadataValue.float(result.maximum),
                "seconds": dg.MetadataValue.float(round(result.seconds, 2)),
                **{
                    f"seconds_{stage}": dg.MetadataValue.float(round(s, 2)) for stage, s in result.stage_seconds.items()
                },
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name="data_contract",
                    passed=True,
                    metadata={"missing_fraction": dg.MetadataValue.float(result.missing_fraction)},
                )
            ],
        )

    return ingestion_asset


h3_lookup = dg.AssetSpec(
    key=dg.AssetKey(["era5", "h3_lookup"]),
    group_name=INGESTION_GROUP,
    kinds={"parquet"},
    description="Grid point to H3 cell lookup table, computed by the first ingestion and then reused.",
)


class Era5DbtTranslator(DagsterDbtTranslator):
    """Models keyed by schema and name (marts/fct_daily_cell_weather) and grouped by layer."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "model":
            return dg.AssetKey([dbt_resource_props["config"]["schema"], dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if dbt_resource_props["resource_type"] == "model":
            return dbt_resource_props["config"]["schema"]
        return super().get_group_name(dbt_resource_props)


def build_dbt_assets(partitions_def: dg.PartitionsDefinition) -> dg.AssetsDefinition:
    """
    The dbt models, partitioned by day like the ingestion.

    A run builds the days of its partitions (start_date/end_date vars of the incremental models);
    a backfill of many days runs dbt once, over the whole range.
    """

    @dbt_assets(
        manifest=dbt_project.manifest_path,
        project=dbt_project,
        partitions_def=partitions_def,
        backfill_policy=dg.BackfillPolicy.single_run(),
        dagster_dbt_translator=Era5DbtTranslator(),
    )
    def era5_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource, era5: Era5PipelineResource):
        window = context.partition_time_window
        dbt_vars = {
            "start_date": window.start.date().isoformat(),
            "end_date": (window.end - timedelta(days=1)).date().isoformat(),
        }
        with environment(era5.dbt_env()):
            yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()

    return era5_dbt_models
