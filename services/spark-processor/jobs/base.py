"""Medallion job hierarchy.

``MedallionJob`` owns the shared read -> transform -> validate -> write lifecycle
(a template method); each concrete job supplies only what differs. This is where
cross-cutting concerns (checksum + lineage, retries/alerts) attach in one place
instead of being copied per stage.
"""

import hashlib
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from catalog import CatalogClient, Checksum, Lineage, SchemaRegistration

from .checksums import dataframe_checksum
from .common import landing_prefix, rolling_window_days, schema_version, table_path
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
    # Layer boundary this job crosses, stamped on the lineage record.
    from_layer: str = ""
    to_layer: str = ""
    # Whether to cache the input DataFrame. Worth it when the input is scanned
    # more than once (default validate_upstream checksums it, then transform
    # reads it again). LandToBronzeJob sets this False: its checksum is over the
    # raw bytes (a separate binaryFile read), so the CSV input is scanned once.
    cache_input: bool = True

    def __init__(self, spark: SparkSession, source: SourceSpec, day: int):
        self.spark = spark
        self.source = source
        self.day = day

    def governance_records(
        self, count: int, columns: list[str]
    ) -> tuple[Lineage, SchemaRegistration]:
        """Build the lineage + schema-registry records for this write (pure)."""
        version = schema_version()
        lineage = Lineage(
            source=self.source.name,
            day=self.day,
            from_layer=self.from_layer,
            to_layer=self.to_layer,
            record_count=count,
            schema_version=version,
        )
        schema = SchemaRegistration(
            source=self.source.name,
            layer=self.to_layer,
            schema_version=version,
            columns=list(columns),
        )
        return lineage, schema

    def checksum_record(self, checksum: str, count: int) -> Checksum:
        """Build the content-checksum record for this write (pure)."""
        return Checksum(
            layer=self.to_layer,
            source=self.source.name,
            day=self.day,
            checksum=checksum,
            record_count=count,
        )

    def validate_upstream(self, catalog: CatalogClient, input_df: DataFrame) -> None:
        """Verify the upstream partition's stored checksum before consuming it.

        Default: recompute the content checksum of the just-read upstream
        DataFrame and compare it to the value the upstream stage recorded. A
        missing record or a mismatch fails the task, so corrupt data cannot
        propagate downstream. ``LandToBronzeJob`` overrides this because its
        upstream (raw landing) is checksummed over bytes, not rows.
        """
        stored = catalog.get_checksum(self.from_layer, self.day, self.source.name)
        if stored is None:
            raise ValueError(
                f"{self.name}: no {self.from_layer} checksum recorded for "
                f"{self.source.name} day={self.day}"
            )
        actual = dataframe_checksum(input_df)
        if actual != stored:
            raise ValueError(
                f"{self.name}: {self.from_layer} checksum mismatch for "
                f"{self.source.name} day={self.day} "
                f"(expected {stored[:12]}..., got {actual[:12]}...)"
            )

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
        """Execute the stage and return the row count written.

        The output ``result`` is cached because three actions consume it
        (``count`` for the empty-guard/lineage, ``write``, and the content
        checksum); without caching Spark would recompute the whole transform
        three times. The input is cached too when ``cache_input`` is set (it is
        scanned by ``validate_upstream`` and again by ``transform``). Both are
        unpersisted in ``finally`` so a failure never leaks cached blocks.
        """
        with CatalogClient.connect() as catalog:
            input_df = self.read()
            if self.cache_input:
                input_df = input_df.persist()
            try:
                # Validate the upstream checksum before consuming the input, so
                # corrupt/tampered upstream data fails here rather than propagating.
                self.validate_upstream(catalog, input_df)
                result = self.transform(input_df).persist()
                try:
                    count = result.count()
                    if count == 0:
                        raise ValueError(
                            f"{self.name}: 0 rows for {self.source.name} day={self.day}"
                        )
                    self.write(result)
                    # Record checksum + lineage + schema as part of the same task,
                    # after a successful write. A catalog failure fails the task.
                    lineage, schema = self.governance_records(count, result.columns)
                    catalog.record_checksum(
                        self.checksum_record(dataframe_checksum(result), count)
                    )
                    catalog.record_lineage(lineage)
                    catalog.register_schema(schema)
                    print(
                        f"{self.name}: wrote {count:,} rows -> {self.target_path()} "
                        f"(source={self.source.name}, day={self.day})",
                        flush=True,
                    )
                    return count
                finally:
                    result.unpersist()
            finally:
                if self.cache_input:
                    input_df.unpersist()


class LandToBronzeJob(MedallionJob):
    """Job (a): landing -> Bronze."""

    name = "land_to_bronze"
    from_layer = "landing"
    to_layer = "bronze"
    # The CSV input is read once (by transform); the checksum is over the raw
    # object bytes via a separate binaryFile read, so caching the CSV would only
    # waste memory on the largest, single-use input.
    cache_input = False

    def read(self) -> DataFrame:
        src = landing_prefix(self.source.name, self.day)
        print(f"{self.name}: reading {src}", flush=True)
        return self.spark.read.option("header", "false").csv(src)

    def validate_upstream(self, catalog: CatalogClient, input_df: DataFrame) -> None:
        """Validate the raw-landing checksum, taken over the object bytes.

        The simulator records ``sha256`` of the uploaded (gzip) object, so this
        reads the same bytes back via Spark's ``binaryFile`` reader and compares
        digests -- the landing layer is raw bytes, not a row-based table.
        """
        stored = catalog.get_checksum("landing", self.day, self.source.name)
        if stored is None:
            raise ValueError(
                f"{self.name}: no landing checksum recorded for "
                f"{self.source.name} day={self.day}"
            )
        files = (
            self.spark.read.format("binaryFile")
            .load(landing_prefix(self.source.name, self.day))
            .select("content")
            .collect()
        )
        if len(files) != 1:
            raise ValueError(
                f"{self.name}: expected exactly one landing object for "
                f"{self.source.name} day={self.day}, found {len(files)}"
            )
        actual = hashlib.sha256(files[0]["content"]).hexdigest()
        if actual != stored:
            raise ValueError(
                f"{self.name}: landing checksum mismatch for "
                f"{self.source.name} day={self.day} "
                f"(expected {stored[:12]}..., got {actual[:12]}...)"
            )

    def transform(self, df: DataFrame) -> DataFrame:
        return self.source.to_bronze(df)

    def target_path(self) -> str:
        return table_path("bronze", self.source.name)

    def partition_by(self) -> list[str]:
        return ["source", "day"]


class BronzeToSilverJob(MedallionJob):
    """Job (b): Bronze -> Silver."""

    name = "bronze_to_silver"
    from_layer = "bronze"
    to_layer = "silver"

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

    def governance_records(
        self, count: int, columns: list[str], window: int
    ) -> tuple[Lineage, SchemaRegistration]:
        """Build the Silver -> Gold lineage + schema-registry records (pure)."""
        version = schema_version()
        lineage = Lineage(
            source=None,
            day=self.day,
            from_layer="silver",
            to_layer="gold",
            record_count=count,
            schema_version=version,
            window_days=window,
        )
        schema = SchemaRegistration(
            source=self.gold_table,
            layer="gold",
            schema_version=version,
            columns=list(columns),
        )
        return lineage, schema

    def checksum_record(self, checksum: str, count: int, window: int) -> Checksum:
        """Build the Gold partition's content-checksum record (pure)."""
        return Checksum(
            layer="gold",
            source=None,
            day=self.day,
            window_days=window,
            checksum=checksum,
            record_count=count,
        )

    def _validate_silver(self, catalog: CatalogClient, frames: dict[str, DataFrame]) -> None:
        """Verify the stored Silver checksum of every present window day per source.

        Each Bronze -> Silver run records a per-(source, day) checksum; the Gold
        job recomputes it for the days it actually reads and fails on a missing
        record or a mismatch. Days absent from the window are simply not
        validated (there is nothing to read).
        """
        for source in self.sources:
            df = frames[source]
            present_days = sorted({row["day"] for row in df.select("day").distinct().collect()})
            for day in present_days:
                stored = catalog.get_checksum("silver", day, source)
                if stored is None:
                    raise ValueError(
                        f"{self.name}: no silver checksum recorded for {source} day={day}"
                    )
                actual = dataframe_checksum(df.where(F.col("day") == day))
                if actual != stored:
                    raise ValueError(
                        f"{self.name}: silver checksum mismatch for {source} day={day} "
                        f"(expected {stored[:12]}..., got {actual[:12]}...)"
                    )

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

        This approximation is based on partition existence: it cannot distinguish
        "delivered but empty" from "not delivered", so an empty-but-delivered day
        reads as not present.
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

        with CatalogClient.connect() as catalog:
            # Cache each source's windowed Silver: it is scanned by checksum
            # validation (per day), the presence check, and the feature builder.
            # Without caching each source would be re-read from Silver 3+ times.
            frames = {
                src: self._windowed_silver(src, start_day).persist() for src in self.sources
            }
            features = None
            try:
                # Validate every upstream Silver partition before reading it.
                self._validate_silver(catalog, frames)
                source_present = {
                    src: self._source_present(frames[src], expected_days)
                    for src in self.sources
                }

                # Cached: consumed by the empty-guard count, the write, and the
                # content checksum.
                features = assemble_computer_features(
                    auth=self._builders["auth"](frames["auth"]),
                    proc=self._builders["proc"](frames["proc"]),
                    flows=self._builders["flows"](frames["flows"]),
                    dns=self._builders["dns"](frames["dns"]),
                    anchor_day=self.day,
                    window_days=window,
                    source_present=source_present,
                ).persist()

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
                lineage, schema = self.governance_records(count, features.columns, window)
                catalog.record_checksum(
                    self.checksum_record(dataframe_checksum(features), count, window)
                )
                catalog.record_lineage(lineage)
                catalog.register_schema(schema)
                print(
                    f"{self.name}: wrote {count:,} rows -> {target} "
                    f"(anchor_day={self.day}, window={window}d, present={source_present})",
                    flush=True,
                )
                return count
            finally:
                if features is not None:
                    features.unpersist()
                for frame in frames.values():
                    frame.unpersist()
