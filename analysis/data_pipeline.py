"""
NYC TLC Data Processing Pipeline and Lineage Orchestrator.

Executes the ETL pipeline for NYC Taxi data using Polars, integrating deeply
with OpenLineage to automatically track schema, lifecycle, branching (quarantine), 
and column-level lineage.
"""
import datetime
import pathlib
from contextvars import ContextVar
from typing import Any, Optional, Protocol, TypeVar

import polars as pl
import polars.selectors as cs
from openlineage.client import client, facet, run
from openlineage.client.generated import datasource_dataset
from pandera import errors

from .openlineage_tracker import OpenLineageRun
from .polars_mappings import CASTING_MAPPING
from .string_utils import camel_to_snake
from .tlc_schema import (
    DESCRIPTIONS_MAPPING,
    PAYMENT_TYPE_MAPPING,
    RATECODE_MAPPING,
    RENAME_MAPPING,
    REVERSE_RENAME_MAPPING,
    TRIP_TYPE_MAPPING,
    VENDOR_MAPPING,
)
from .tlc_validation import TLC_POLARS_VALIDATION_SCHEMA

Frame = TypeVar("Frame", pl.DataFrame, pl.LazyFrame)

class PipelineStep(Protocol):
    __name__: str
    def __call__(self, frame: Frame, **kwargs: Any) -> Frame: ...

STEP_REGISTRY: dict[str, PipelineStep] = {}

STEPS: list[str] = [
    "rename_columns_to_snake_case",
    "rename_columns_to_canonical",
    "add_origin_columns",
    "cast_types",
    "add_mapped_columns",
    "enrich_with_taxi_zones",
    "clean_data",
    "validate_data",
    "reorder_columns",
]

ctx_run: ContextVar[run.Run] = ContextVar("run")
ctx_job: ContextVar[run.Job] = ContextVar("job")

OpenLineageRun.set_client(client.OpenLineageClient(
    config={
        "transport": {
            "type": "http",
            "url": "http://localhost:5000",
            "auth": {"type": "api_key", "apiKey": "..."}
        }
    }
))
OpenLineageRun.set_producer(__file__)

def get_schema(frame: Frame) -> pl.Schema:
    return frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema

def schema_to_facet(schema: pl.Schema) -> facet.SchemaDatasetFacet:
    return facet.SchemaDatasetFacet(
        fields=[
            facet.SchemaField(**{  # type: ignore
                "name": name,
                "type": str(dtype),
                "description": DESCRIPTIONS_MAPPING.get(RENAME_MAPPING.get(camel_to_snake(name), name), None)
            })
            for name, dtype in schema.items()
        ])

def build_dataset_facets(
    frame: Frame, 
    is_output: bool = False,
    file_format: str = "parquet",
    datasource_uri: str = "memory://",
    datasource_name: str = "in_memory_processing"
) -> dict:
    facets = {
        "schema": schema_to_facet(get_schema(frame)),
        "dataSource": datasource_dataset.DatasourceDatasetFacet(name=datasource_name, uri=datasource_uri),
        "storage": facet.StorageDatasetFacet(storageLayer="local", fileFormat=file_format),
        "ownership": facet.OwnershipDatasetFacet(
            owners=[facet.OwnershipDatasetFacetOwners(name="data-engineering-team", type="Team")]
        )
    }

    if is_output:
        facets["lifecycleStateChange"] = facet.LifecycleStateChangeDatasetFacet(
            lifecycleStateChange=facet.LifecycleStateChange.OVERWRITE,
            previousIdentifier=facet.LifecycleStateChangeDatasetFacetPreviousIdentifier(
                name=ctx_job.get().name,
                namespace=ctx_job.get().namespace
            )
        )
        if isinstance(frame, pl.DataFrame):
            facets["outputStatistics"] = facet.OutputStatisticsOutputDatasetFacet(
                rowCount=frame.height,
                size=int(frame.estimated_size())
            )
    return facets

def register_step(
    column_mapping: Optional[dict[str, list[str]]] = None,
    extra_inputs: Optional[list[run.Dataset]] = None 
):
    """Decorator to register a pipeline step and track its lineage."""
    def decorator(func: PipelineStep) -> PipelineStep:
        def wrapper(frame: Frame, **kwargs: Any) -> Frame:
            parent_run = ctx_run.get()
            parent_job = ctx_job.get()

            with OpenLineageRun("step", func.__name__) as ol_run:
                ol_run.run.facets["parent"] = facet.ParentRunFacet.create(
                    runId=parent_run.runId, name=parent_job.name, namespace=parent_job.namespace
                )
                
                base_inputs = [run.Dataset(
                    namespace="inmemory://", name=parent_job.name,
                    facets=build_dataset_facets(frame, is_output=False)
                )]
                if extra_inputs:
                    base_inputs.extend(extra_inputs)
                    
                ol_run.inputs = base_inputs
                ol_run.start()
                
                # Create a mutable list for steps to inject branching outputs
                step_extra_outputs: list[run.Dataset] = []
                kwargs["extra_outputs"] = step_extra_outputs 
                
                # Execute the step
                result = func(frame, **kwargs)
                
                output_facets = build_dataset_facets(result, is_output=True)
                
                if column_mapping:
                    lineage_fields = {}
                    for out_col, in_cols in column_mapping.items():
                        lineage_fields[out_col] = facet.ColumnLineageDatasetFacetFieldsAdditional(
                            inputFields=[  # type: ignore
                                facet.ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                                    namespace=parent_job.namespace, name=parent_job.name, field=in_col
                                ) for in_col in in_cols
                            ],
                            transformationDescription=f"Transformed in {func.__name__}",
                            transformationType="TRANSFORMATION" 
                        )
                    output_facets["columnLineage"] = facet.ColumnLineageDatasetFacet(fields=lineage_fields)

                ol_run.outputs = [run.Dataset(
                    namespace="inmemory://", name=parent_job.name, facets=output_facets
                )] + step_extra_outputs
                
                return result

        STEP_REGISTRY[func.__name__] = wrapper
        return wrapper
    return decorator


def get_steps() -> list[PipelineStep]:
    try:
        return [STEP_REGISTRY[name] for name in STEPS]
    except KeyError as exc:
        valid = ", ".join(sorted(STEP_REGISTRY))
        raise ValueError(f"Unknown policy name: {exc.args[0]!r}. Valid names: {valid}") from exc

def run_steps(frame: Frame, name: str, *, steps: Optional[list[PipelineStep]] = None, **kwargs: Any) -> Frame:
    resolved_steps = steps or get_steps()
    parent_job = ctx_job.get()
    parent_run = ctx_run.get()
    
    with (
        OpenLineageRun("run_steps", name) as ol_run,
        ctx_job.set(ol_run.job),  # type: ignore
        ctx_run.set(ol_run.run)   # type: ignore
    ):
        ol_run.run.facets["parent"] = facet.ParentRunFacet.create(
            runId=parent_run.runId, name=parent_job.name, namespace=parent_job.namespace
        )
        ol_run.inputs = [run.Dataset(
            namespace="file", name=name,
            facets=build_dataset_facets(frame, is_output=False, datasource_uri=f"file://{name}.parquet", datasource_name="local")
        )]
        
        ol_run.start()
        
        current_frame = frame
        for step in resolved_steps:
            current_frame = step(current_frame, **kwargs)
            
        ol_run.outputs = [run.Dataset(
            namespace="inmemory://", name=name,
            facets=build_dataset_facets(current_frame, is_output=True)
        )]
        return current_frame

def run_pipeline(files: list[str], namespace: str, name: str, *, zone_lookup: pl.LazyFrame) -> pl.DataFrame:
    # Lazily evaluate the dimension table for broadcast joining
    zone_lookup_lazy = (
        zone_lookup.rename(camel_to_snake)
        .with_columns(
            pl.col("location_id").cast(pl.Int32),
            pl.col("borough").cast(pl.Categorical),
            pl.col("zone").cast(pl.Categorical),
            pl.col("service_zone").cast(pl.Categorical)
        )
    )

    file_list: list[tuple[pl.LazyFrame, str]] = []
    match files:
        case list():
            file_list = [(pl.scan_parquet(file), pathlib.Path(file).stem) for file in files]
        case str():
            file_list = [(pl.scan_parquet(files), pathlib.Path(files).stem)]
        case _:
            raise NotImplementedError("Implemented only for local files")
    
    with (
        OpenLineageRun(namespace, name) as ol_run,
        ctx_job.set(ol_run.job),  # type: ignore
        ctx_run.set(ol_run.run)   # type: ignore
    ):  
        ol_run.inputs = [
            run.Dataset(
                namespace="file", name=stem,
                facets=build_dataset_facets(frame, is_output=False, datasource_uri=f"file://{stem}.parquet", datasource_name="local")
            )
            for frame, stem in file_list
        ]
        
        ol_run.start()
        
        frames: list[pl.LazyFrame] = [
            run_steps(frame, stem, file_stem=stem, zone_lookup=zone_lookup_lazy)
            for frame, stem in file_list
        ]
        if len(frames) > 1:
            df = pl.concat(frames, how="diagonal_relaxed")
            collected_df = df.collect()
        else:
            collected_df = frames.pop().collect()

        ol_run.outputs = [
            run.Dataset(
                namespace="inmemory://", name=name,
                facets=build_dataset_facets(collected_df, is_output=True)
            )
        ]
        return collected_df


@register_step()
def rename_columns_to_snake_case(frame: Frame, **_) -> Frame:
    return frame.rename(camel_to_snake)

@register_step(column_mapping=REVERSE_RENAME_MAPPING)
def rename_columns_to_canonical(frame: Frame, **_) -> Frame:
    return frame.rename(lambda col: RENAME_MAPPING.get(col, col))


@register_step()
def add_origin_columns(frame: Frame, *, file_stem: str, **_) -> Frame:
    if "yellow" in file_stem:
        origin_val, origin_id_val = "TPEP", 1
    elif "green" in file_stem:
        origin_val, origin_id_val = "LPEP", 2
    else:
        origin_val, origin_id_val = "UNKNOWN", 99

    return frame.with_columns(
        pl.lit(origin_val).cast(pl.Categorical).alias("origin"),
        pl.lit(origin_id_val).cast(pl.Int8).alias("origin_id")
    )

@register_step()
def cast_types(frame: Frame, **_) -> Frame:
    return frame.with_columns(
        *[cs.by_name(name, require_all=False).cast(dtype)
          for name, dtype in CASTING_MAPPING.items()]
    )

@register_step()
def add_mapped_columns(frame: Frame, **_) -> Frame:
    return frame.with_columns(
        *[
            cs.by_name(name, require_all=False)\
            .alias(name.replace("_id", ""))\
            .replace(mapping, default=None)\
            .cast(pl.Categorical)
            for name, mapping in {
                "ratecode_id": RATECODE_MAPPING,
                "vendor_id": VENDOR_MAPPING,
                "payment_type_id": PAYMENT_TYPE_MAPPING,
                "trip_type_id": TRIP_TYPE_MAPPING
            }.items()
        ]
    )


@register_step(extra_inputs=[run.Dataset(namespace="file", name="taxi_zone_lookup.csv")])
def enrich_with_taxi_zones(frame: Frame, *, zone_lookup: Frame, **_) -> Frame:
    return frame.join(
        zone_lookup.rename(lambda col: "pickup_" + col),  # type: ignore
        on="pickup_location_id"
    ).join(
        zone_lookup.rename(lambda col: "dropoff_" + col),  # type: ignore
        on="dropoff_location_id"
    )

@register_step()
def reorder_columns(frame: Frame, **_) -> Frame:
    if isinstance(frame, pl.LazyFrame):
        return frame.select(sorted(frame.collect_schema().names()))
    return frame.select(sorted(frame.columns))

@register_step()
def clean_data(frame: Frame, **_) -> Frame:
    return (
        frame
        # Keep unique rows only, as duplicates indicate data quality issues
        .unique()

        # Filtering out trips with invalid passenger count, which should be between 0 and 6 according to TLC documentation
        .filter(pl.col("passenger_count").is_not_null() & (pl.col("passenger_count") >= 0) & (pl.col("passenger_count") <= 6))

        # Filtering out trips with invalid ratecode_id, which should be between 1 and 6 according to TLC documentation
        .filter(pl.col("ratecode_id").is_not_null() & pl.col("ratecode_id") != 99)

        # Filtering out trips with invalid pickup and dropoff datetimes, ensuring positive duration and not exceeding 12 hours
        .filter((
            (pl.col("dropoff_datetime") > pl.col("pickup_datetime")) & 
            ((pl.col("dropoff_datetime") - pl.col("pickup_datetime")) <= datetime.timedelta(hours=12))
        ))

        # Filtering out trips flagged as 'No charge' but having a generated metered fare amount, which indicates a data inconsistency
        .filter(~((pl.col("payment_type_id") == 3) & (pl.col("fare_amount") > 0.0)))

        # Filtering out trips with 0.0 miles but charging a fare, which indicates a defective taximeter, unless it is a Negotiated Fare
        .filter(~((pl.col("ratecode_id") != 5) & (pl.col("trip_distance_mi") == 0.0) & (pl.col("fare_amount") > 0.0)))

        # Filtering out trips with an average speed greater than 70 mph, which is unrealistic for NYC traffic conditions
        .with_columns((pl.col("dropoff_datetime") - pl.col("pickup_datetime")).alias("trip_elapsed_time"))
        .with_columns((pl.col("trip_distance_mi") / pl.col("trip_elapsed_time").dt.total_seconds()).alias("mph") * 3600)
        .filter(~(pl.col("mph") > 70))
        .drop("trip_elapsed_time", "mph")

        # Filter out empty columns
        .drop("ehail_fee", strict=False)
    )


@register_step()
def validate_data(frame: Frame, **_) -> Frame:
    try:
        match frame:
            case pl.DataFrame():
                return TLC_POLARS_VALIDATION_SCHEMA.validate(frame, lazy=True)
            case pl.LazyFrame():
                return TLC_POLARS_VALIDATION_SCHEMA.validate(frame.collect(), lazy=True).lazy()
    except (errors.SchemaErrors, errors.SchemaError) as err:
        raise ValueError(f"Data validation failed: {err.failure_cases}") from err
