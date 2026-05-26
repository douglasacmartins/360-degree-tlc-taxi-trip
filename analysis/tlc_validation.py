import datetime

import pandera.polars as pa
import polars as pl

from analysis.tlc_schema import PAYMENT_TYPE_MAPPING, RATECODE_MAPPING, VENDOR_MAPPING


def not_empty(df: pa.PolarsData) -> pl.LazyFrame:
    return df.lazyframe.select(~pl.all().is_null().all())

def valid_trip_duration(df: pa.PolarsData) -> pl.LazyFrame:
    """TLC regulations require that trips must have a positive duration and cannot exceed the 12-hour legal limit."""
    return df.lazyframe.select(
        result = (
            (pl.col("dropoff_datetime") > pl.col("pickup_datetime")) & 
            ((pl.col("dropoff_datetime") - pl.col("pickup_datetime")) <= datetime.timedelta(hours=12))
        ).all()
    )

def valid_no_charge_fare(df: pa.PolarsData) -> pl.LazyFrame:
    """TLC regulations specify that trips flagged as 'No charge' must not have a generated metered fare amount."""
    return df.lazyframe.select(
        result = ~((pl.col("payment_type_id") == 3) & (pl.col("fare_amount") > 0.0)).any()
    )

def valid_negotiated_fare(df: pa.PolarsData) -> pl.LazyFrame:
    """TLC regulations state that trips recording 0.0 miles but charging a fare indicate a defective taximeter, and it must be a Negotiated Fare. Any exception must be considered an TLC violation and investigated accordingly."""
    return df.lazyframe.select(
        result = ~((pl.col("ratecode_id") != 5) & (pl.col("trip_distance_mi") == 0.0) & (pl.col("fare_amount") > 0.0)).any()
    )


TLC_POLARS_VALIDATION_SCHEMA = pa.DataFrameSchema(
    columns={
        ".*": pa.Column(None, pa.Check(not_empty), nullable=True, regex=True),
        "vendor_id": pa.Column(pl.Int8, pa.Check.isin(list(VENDOR_MAPPING.keys())), nullable=False), 
        "pickup_datetime": pa.Column(pl.Datetime, nullable=False),
        "dropoff_datetime": pa.Column(pl.Datetime, nullable=False),
        "passenger_count": pa.Column(pl.Int8, pa.Check.in_range(0, 6), nullable=False),
        "trip_distance_mi": pa.Column(pl.Float64, pa.Check.greater_than_or_equal_to(0.0), nullable=False),
        "ratecode_id": pa.Column(pl.Int8, pa.Check.isin(list(RATECODE_MAPPING.keys())), nullable=False),
        "fare_amount": pa.Column(pl.Float64, nullable=False),
        "payment_type_id": pa.Column(pl.Int8, pa.Check.isin(list(PAYMENT_TYPE_MAPPING.keys())), nullable=False, required=False),
        "total_amount": pa.Column(pl.Float64, nullable=False)
    },
    checks=[
        pa.Check(
            valid_trip_duration,
            name="valid_trip_duration",
            error="Trips must have a positive duration and cannot exceed the 12-hour TLC legal limit."
        ),
        pa.Check(
            valid_no_charge_fare,
            name="valid_no_charge_fare",
            error="Trips flagged as 'No charge' must not have a generated metered fare amount."
        ),
        pa.Check(
            valid_negotiated_fare,
            name="valid_negotiated_fare",
            error=(
                "Trips recording 0.0 miles but charging a fare indicate a defective taximeter, and it must be a Negotiated Fare."
                "Any exception must be considered an TLC violation and investigated accordingly."
            )
        )
    ]
)
