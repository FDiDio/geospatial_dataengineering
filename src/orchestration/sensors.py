import dagster as dg

from orchestration.resources import Era5PipelineResource
from utils.variables import get_variable

# Partitions checked on the first tick, when the sensor has no cursor yet
FIRST_TICK_LOOKBACK_DAYS = 10
MAX_RUNS_PER_TICK = 7


def build_availability_sensor(
    job_name: str, variable_names: list[str], partitions_def: dg.DailyPartitionsDefinition
) -> dg.SensorDefinition:
    """
    Launch the daily partitions as soon as ERA5 publishes all the fields they need.

    ERA5 is released with a latency of about 6 days: a fixed schedule would run before the data
    exists. The sensor checks the source every few hours and requests the new days in order.
    """
    fields = {field for name in variable_names for field in get_variable(name).fields}

    @dg.sensor(
        job_name=job_name,
        minimum_interval_seconds=6 * 3600,
        default_status=dg.DefaultSensorStatus.STOPPED,
        description="Runs the ingestion and the dbt models of each new day once ERA5 has published it.",
    )
    def era5_availability_sensor(context: dg.SensorEvaluationContext, era5: Era5PipelineResource):
        source = era5.pipeline().source
        partition_keys = partitions_def.get_partition_keys()
        if context.cursor in partition_keys:
            candidates = partition_keys[partition_keys.index(context.cursor) + 1 :]
        else:
            candidates = partition_keys[-FIRST_TICK_LOOKBACK_DAYS:]

        cursor = context.cursor
        for key in candidates[:MAX_RUNS_PER_TICK]:
            year, month, day = (int(part) for part in key.split("-"))
            if not all(source.available(field, year, month, day) for field in fields):
                break  # days are published in order
            yield dg.RunRequest(run_key=key, partition_key=key)
            cursor = key

        if cursor != context.cursor:
            context.update_cursor(cursor)
        else:
            yield dg.SkipReason(f"No new day published after {cursor or candidates[0]}")

    return era5_availability_sensor
