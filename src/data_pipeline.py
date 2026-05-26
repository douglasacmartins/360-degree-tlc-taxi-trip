"""
NYC TLC Data Processing Pipeline.

Executes the ETL pipeline for NYC Taxi data using PySpark.
Lineage (schema, lifecycle, branching, and column-level tracking) is 
handled automatically by the OpenLineageSparkListener at the cluster level.
"""
import pathlib
from typing import Any, Optional, Protocol

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pandera import errors
from pyspark.sql import DataFrame, SparkSession

from spark_mappings import CASTING_MAPPING
from string_utils import camel_to_snake
from tlc_schema import (
    DESCRIPTIONS_MAPPING,
    PAYMENT_TYPE_MAPPING,
    RATECODE_MAPPING,
    RENAME_MAPPING,
    TRIP_TYPE_MAPPING,
    VENDOR_MAPPING,
)
from tlc_validation import TLC_PYSPARK_VALIDATION_SCHEMA


class PipelineStep(Protocol):
    __name__: str
    def __call__(self, frame: DataFrame, **kwargs: Any) -> DataFrame: ...


STEP_REGISTRY: dict[str, PipelineStep] = {}

# The order of execution for the pipeline
STEPS: list[str] = [
    "attach_data_dictionary",
    "rename_columns_to_snake_case",
    "rename_columns_to_canonical",
    "add_origin_columns",
    "cast_types",
    "add_mapped_columns",
    "enrich_with_taxi_zones",
    "clean_data",
    "validate_data",
    "reorder_columns",
    "attach_data_dictionary"
]


def register_step():
    """Decorator to register a pipeline step."""
    def decorator(func: PipelineStep) -> PipelineStep:
        STEP_REGISTRY[func.__name__] = func
        return func
    return decorator


def get_steps() -> list[PipelineStep]:
    try:
        return [STEP_REGISTRY[name] for name in STEPS]
    except KeyError as exc:
        valid = ", ".join(sorted(STEP_REGISTRY))
        raise ValueError(f"Unknown step name: {exc.args[0]!r}. Valid names: {valid}") from exc


def run_steps(frame: DataFrame, *, steps: Optional[list[PipelineStep]] = None, **kwargs: Any) -> DataFrame:
    """Sequentially applies registered steps to the DataFrame."""
    resolved_steps = steps or get_steps()
    
    current_frame = frame
    for step in resolved_steps:
        current_frame = step(current_frame, **kwargs)
        
    return current_frame


def run_pipeline(spark: SparkSession, files: list[str] | str, *, zone_lookup: DataFrame) -> DataFrame:
    """Orchestrates the pipeline execution across multiple input files."""
    # Prepare dimension table
    zone_lookup_formatted = zone_lookup
    for col_name in zone_lookup.columns:
        zone_lookup_formatted = zone_lookup_formatted.withColumnRenamed(col_name, camel_to_snake(col_name))
        
    zone_lookup_formatted = zone_lookup_formatted.withColumns({
        "location_id": F.col("location_id").cast(T.IntegerType()),
        "borough": F.col("borough").cast(T.StringType()),
        "zone": F.col("zone").cast(T.StringType()),
        "service_zone": F.col("service_zone").cast(T.StringType())
    })

    file_list: list[tuple[DataFrame, str]] = []
    if isinstance(files, list):
        file_list = [(spark.read.parquet(file), pathlib.Path(file).stem) for file in files]
    elif isinstance(files, str):
        file_list = [(spark.read.parquet(files), pathlib.Path(files).stem)]
    else:
        raise NotImplementedError("Implemented only for local files as list or str")
    
    frames: list[DataFrame] = [
        run_steps(frame, file_stem=stem, zone_lookup=zone_lookup_formatted)
        for frame, stem in file_list
    ]
    
    if len(frames) > 1:
        # Unioning DataFrames by name
        final_df = frames[0]
        for df in frames[1:]:
            final_df = final_df.unionByName(df, allowMissingColumns=True)
    else:
        final_df = frames.pop()

    return final_df


@register_step()
def rename_columns_to_snake_case(frame: DataFrame, **_) -> DataFrame:
    for col_name in frame.columns:
        frame = frame.withColumnRenamed(col_name, camel_to_snake(col_name))
    return frame

@register_step()
def rename_columns_to_canonical(frame: DataFrame, **_) -> DataFrame:
    for col_name in frame.columns:
        if col_name in RENAME_MAPPING:
            frame = frame.withColumnRenamed(col_name, RENAME_MAPPING[col_name])
    return frame

@register_step()
def add_origin_columns(frame: DataFrame, *, file_stem: str, **_) -> DataFrame:
    if "yellow" in file_stem:
        origin_val, origin_id_val = "TPEP", 1
    elif "green" in file_stem:
        origin_val, origin_id_val = "LPEP", 2
    else:
        origin_val, origin_id_val = "UNKNOWN", 99

    return frame.withColumn("origin", F.lit(origin_val).cast(T.StringType())) \
                .withColumn("origin_id", F.lit(origin_id_val).cast(T.ByteType()))

@register_step()
def cast_types(frame: DataFrame, **_) -> DataFrame:
    for name, dtype in CASTING_MAPPING.items():
        if name in frame.columns:
            frame = frame.withColumn(name, F.col(name).cast(dtype))
    return frame

@register_step()
def add_mapped_columns(frame: DataFrame, **_) -> DataFrame:
    mappings = {
        "ratecode_id": RATECODE_MAPPING,
        "vendor_id": VENDOR_MAPPING,
        "payment_type_id": PAYMENT_TYPE_MAPPING,
        "trip_type_id": TRIP_TYPE_MAPPING
    }
    
    for name, mapping in mappings.items():
        if name in frame.columns:
            new_col_name = name.replace("_id", "")
            map_expr = F.create_map([F.lit(x) for k, v in mapping.items() for x in (k, v)])
            frame = frame.withColumn(new_col_name, map_expr.getItem(F.col(name)).cast(T.StringType()))
    return frame

@register_step()
def enrich_with_taxi_zones(frame: DataFrame, *, zone_lookup: DataFrame, **_) -> DataFrame:
    pickup_zones = zone_lookup.select([F.col(c).alias("pickup_" + c) for c in zone_lookup.columns])
    dropoff_zones = zone_lookup.select([F.col(c).alias("dropoff_" + c) for c in zone_lookup.columns])

    frame = frame.join(
        F.broadcast(pickup_zones),
        frame["pickup_location_id"] == pickup_zones["pickup_location_id"],
        "left"
    ).drop(pickup_zones["pickup_location_id"])

    frame = frame.join(
        F.broadcast(dropoff_zones),
        frame["dropoff_location_id"] == dropoff_zones["dropoff_location_id"],
        "left"
    ).drop(dropoff_zones["dropoff_location_id"])

    return frame

@register_step()
def clean_data(frame: DataFrame, **_) -> DataFrame:
    return (
        frame
        .dropDuplicates()
        .filter(F.col("passenger_count").isNotNull() & (F.col("passenger_count") >= 0) & (F.col("passenger_count") <= 6))
        .filter(F.col("ratecode_id").isNotNull() & (F.col("ratecode_id") != 99))
        .filter(
            (F.col("dropoff_datetime") > F.col("pickup_datetime")) & 
            ((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) <= 12 * 3600)
        )
        .filter(~((F.col("payment_type_id") == 3) & (F.col("fare_amount") > 0.0)))
        .filter(~((F.col("ratecode_id") != 5) & (F.col("trip_distance_mi") == 0.0) & (F.col("fare_amount") > 0.0)))
        .withColumn("trip_elapsed_seconds", F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime"))
        .withColumn("mph", (F.col("trip_distance_mi") / F.col("trip_elapsed_seconds")) * 3600)
        .filter(~(F.col("mph") > 70))
        .drop("trip_elapsed_seconds", "mph", "ehail_fee")
    )

@register_step()
def attach_data_dictionary(frame: DataFrame, **_) -> DataFrame:
    """
    Attaches column descriptions to Spark's internal metadata.
    The OpenLineageSparkListener will automatically extract these 
    and populate the schema facet descriptions.
    """
    for col_name in frame.columns:
        description = DESCRIPTIONS_MAPPING.get(RENAME_MAPPING.get(col_name, col_name), None)
        if description:
            frame = frame.withColumn(
                col_name, 
                F.col(col_name).alias(col_name, metadata={"comment": description})
            )
    return frame

@register_step()
def validate_data(frame: DataFrame, **_) -> DataFrame:
    try:
        return TLC_PYSPARK_VALIDATION_SCHEMA.validate(frame)
    except (errors.SchemaErrors, errors.SchemaError) as err:
        raise ValueError(f"Data validation failed: {err.failure_cases}") from err

@register_step()
def reorder_columns(frame: DataFrame, **_) -> DataFrame:
    return frame.select(sorted(frame.columns))