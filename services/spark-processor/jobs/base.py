"""Medallion job hierarchy.

``MedallionJob`` owns the shared read -> transform -> validate -> write lifecycle
(a template method); each concrete job supplies only what differs. This is where
cross-cutting concerns (checksum + lineage, retries/alerts) attach in one place
instead of being copied per stage.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import landing_prefix, rolling_window_days, table_path
from .sources import SourceSpec
from .transforms import (
    assemble_computer_features,
    auth_computer_features,
    dns_computer_features,
    flows_computer_features,
    proc_computer_features,
)


class MedallionJob(ABC):
    """Template for a single Medallion transition for one source and day."""

    name: str = "medallion_job"

    def __init__(self, spark: SparkSession, source: SourceSpec, day: int):
        self.spark = spark
        self.source = source
        self.day = day

    @abstractmethod
    def read(self) -> DataFrame:
        """Read the input DataFrame for this stage."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply this stage's (pure) transform."""

    @abstractmethod
    def target_path(self) -> str:
        """Return the Delta output path."""

    def partition_by(self) -> list[str]:
        """Partition columns for the output table."""
        return ["day"]

    def write(self, df: DataFrame) -> None:
        """Write the output as a partitioned Delta table (dynamic overwrite)."""
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(*self.partition_by())
            .save(self.target_path())
        )

    def run(self) -> int:
        """Execute the stage and return the row count written."""
        result = self.transform(self.read())
        count = result.count()
        if count == 0:
            raise ValueError(f"{self.name}: 0 rows for {self.source.name} day={self.day}")
        self.write(result)
        print(
            f"{self.name}: wrote {count:,} rows -> {self.target_path()} "
            f"(source={self.source.name}, day={self.day})",
            flush=True,
        )
        return count


class LandToBronzeJob(MedallionJob):
    """Job (a): landing -> Bronze."""

    name = "land_to_bronze"

    def read(self) -> DataFrame:
        src = landing_prefix(self.source.name, self.day)
        print(f"{self.name}: reading {src}", flush=True)
        return self.spark.read.option("header", "false").csv(src)

    def transform(self, df: DataFrame) -> DataFrame:
        return self.source.to_bronze(df)

    def target_path(self) -> str:
        return table_path("bronze", self.source.name)

    def partition_by(self) -> list[str]:
        return ["source", "day"]


class BronzeToSilverJob(MedallionJob):
    """Job (b): Bronze -> Silver."""

    name = "bronze_to_silver"

    def read(self) -> DataFrame:
        path = table_path("bronze", self.source.name)
        print(f"{self.name}: reading {path} (day={self.day})", flush=True)
        return (
            self.spark.read.format("delta")
            .load(path)
            .where((F.col("source") == self.source.name) & (F.col("day") == self.day))
        )

    def transform(self, df: DataFrame) -> DataFrame:
        return self.source.to_silver(df)

    def target_path(self) -> str:
        return table_path("silver", self.source.name)


JOB_CLASSES: dict[str, type[MedallionJob]] = {
    LandToBronzeJob.name: LandToBronzeJob,
    BronzeToSilverJob.name: BronzeToSilverJob,
}


class ComputerFeaturesJob:
    """Cross-source Silver -> Gold job building the unified per-computer feature table.

    This is the single Gold job: it reads *all four* Silver tables over the same
    rolling window (anchor day plus the preceding ``ROLLING_WINDOW_DAYS - 1``),
    computes per-source per-computer features, and joins them into one row per
    computer per ``(anchor_day, window_days)`` -- the data-science feature contract.
    Written to the ``computer_features`` Gold table, partitioned by
    ``(window_days, anchor_day)`` with dynamic overwrite so a re-run (or a different
    window length) replaces only its own partition without double counting.
    """

    name = "silver_to_gold"
    gold_table = "computer_features"
    sources = ("auth", "proc", "flows", "dns")
    _builders = {
        "auth": auth_computer_features,
        "proc": proc_computer_features,
        "flows": flows_computer_features,
        "dns": dns_computer_features,
    }

    def __init__(self, spark: SparkSession, day: int):
        self.spark = spark
        self.day = day

    def _windowed_silver(self, source: str, start_day: int) -> DataFrame:
        """Read one source's Silver rows for the window [start_day, anchor day]."""
        path = table_path("silver", source)
        return (
            self.spark.read.format("delta")
            .load(path)
            .where(F.col("day").between(start_day, self.day))
        )

    @staticmethod
    def _source_present(df: DataFrame, expected_days: set[int]) -> bool:
        """Approximate availability: every expected window day has at least one row.

        This is a stand-in until the governance catalog records true
        per-day delivery. It cannot distinguish "delivered but empty" from "not
        delivered", so an empty-but-delivered day reads as not present.
        """
        present_days = {row["day"] for row in df.select("day").distinct().collect()}
        return expected_days.issubset(present_days)

    def run(self) -> int:
        """Build and write the unified per-computer feature partition; return row count."""
        window = rolling_window_days()
        start_day = self.day - window + 1
        # Window days that can actually exist (day indices are non-negative).
        expected_days = set(range(max(0, start_day), self.day + 1))
        print(
            f"{self.name}: anchor_day={self.day}, window={window}d "
            f"-> days {start_day}..{self.day}",
            flush=True,
        )

        frames = {src: self._windowed_silver(src, start_day) for src in self.sources}
        source_present = {
            src: self._source_present(frames[src], expected_days) for src in self.sources
        }

        features = assemble_computer_features(
            auth=self._builders["auth"](frames["auth"]),
            proc=self._builders["proc"](frames["proc"]),
            flows=self._builders["flows"](frames["flows"]),
            dns=self._builders["dns"](frames["dns"]),
            anchor_day=self.day,
            window_days=window,
            source_present=source_present,
        )

        count = features.count()
        if count == 0:
            raise ValueError(f"{self.name}: 0 rows for anchor_day={self.day}")

        target = table_path("gold", self.gold_table)
        (
            features.write.format("delta")
            .mode("overwrite")
            .partitionBy("window_days", "anchor_day")
            .save(target)
        )
        print(
            f"{self.name}: wrote {count:,} rows -> {target} "
            f"(anchor_day={self.day}, window={window}d, present={source_present})",
            flush=True,
        )
        return count
