"""Medallion job hierarchy.

``MedallionJob`` owns the shared read -> transform -> validate -> write lifecycle
(a template method); each concrete job supplies only what differs. This is where
cross-cutting concerns (checksum + lineage in Increment 5, retries/alerts in
Increment 7) attach in one place instead of being copied per stage.
"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .common import landing_prefix, table_path
from .sources import SourceSpec


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


class SilverToGoldJob(MedallionJob):
    """Job (c): Silver -> Gold (single-day window in Increment 1)."""

    name = "silver_to_gold"

    def read(self) -> DataFrame:
        path = table_path("silver", self.source.name)
        print(f"{self.name}: reading {path} (day={self.day})", flush=True)
        return self.spark.read.format("delta").load(path).where(F.col("day") == self.day)

    def transform(self, df: DataFrame) -> DataFrame:
        return self.source.to_gold(df)

    def target_path(self) -> str:
        return table_path("gold", self.source.gold_table)


JOB_CLASSES: dict[str, type[MedallionJob]] = {
    LandToBronzeJob.name: LandToBronzeJob,
    BronzeToSilverJob.name: BronzeToSilverJob,
    SilverToGoldJob.name: SilverToGoldJob,
}
