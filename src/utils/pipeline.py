import logging
import time
from dataclasses import dataclass, field
from typing import Any

from utils.data_extractor import extract_variable_data
from utils.data_writer import Sink, build_sink
from utils.h3_processor import H3Enricher
from utils.quality import validate_variable_day
from utils.sources import Source, build_source
from utils.variables import VariableSpec

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class VariableDayResult:
    """Summary of a processed variable-day, logged by the CLI and attached to the Dagster materializations."""

    variable: str
    date: str
    rows: int
    missing_fraction: float
    minimum: float
    maximum: float
    stage_seconds: dict[str, float] = field(default_factory=dict)

    @property
    def seconds(self) -> float:
        return sum(self.stage_seconds.values())


@dataclass
class Pipeline:
    """
    The processing of a variable-day: extract, index with H3, validate against the data contract, write.

    One instance is shared by all the workers of a run: the source, the sink and the H3 enricher are thread-safe.
    """

    source: Source
    sink: Sink
    enricher: H3Enricher
    max_retries: int = 3
    max_null_fraction: float = 0.01

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "Pipeline":
        return cls(
            source=build_source(config),
            sink=build_sink(config),
            enricher=H3Enricher(config["h3_resolution"], config["h3_lookup_path"]),
            max_retries=config["max_retries"],
            max_null_fraction=config["max_null_fraction"],
        )

    def process(self, variable: VariableSpec, year: int, month: int, day: int) -> VariableDayResult:
        timings = {}

        start = time.perf_counter()
        df = extract_variable_data(self.source, year, month, day, variable, max_retries=self.max_retries)
        timings["extract"], start = time.perf_counter() - start, time.perf_counter()

        df = self.enricher(df)
        timings["h3"], start = time.perf_counter() - start, time.perf_counter()

        validate_variable_day(df, variable, year, month, day, self.enricher.resolution, self.max_null_fraction)
        timings["validate"], start = time.perf_counter() - start, time.perf_counter()

        self.sink.write(df, variable.name, year, month, day)
        timings["write"] = time.perf_counter() - start

        values = df[variable.short_name]
        result = VariableDayResult(
            variable=variable.name,
            date=f"{year}-{month:02d}-{day:02d}",
            rows=len(df),
            missing_fraction=float(values.isna().mean()),
            minimum=float(values.min()),
            maximum=float(values.max()),
            stage_seconds=timings,
        )
        logger.info(
            "Processed %s %s: %d rows in %.1fs (%s)",
            result.variable, result.date, result.rows, result.seconds,
            ", ".join(f"{stage} {seconds:.1f}s" for stage, seconds in timings.items()),
        )  # fmt: skip
        return result
