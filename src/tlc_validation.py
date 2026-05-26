import pandera.pyspark as pa
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pandera.api.pyspark.types import PysparkDataframeColumnObject
from pandera.extensions import register_check_method

from tlc_schema import PAYMENT_TYPE_MAPPING, RATECODE_MAPPING, VENDOR_MAPPING


@register_check_method
def not_empty(pyspark_obj: PysparkDataframeColumnObject) -> bool:
    """Checks that the DataFrame is not completely empty."""
    df = pyspark_obj.dataframe
    return not df.isEmpty()


@register_check_method
def valid_trip_duration(pyspark_obj: PysparkDataframeColumnObject) -> bool:
    """TLC trips must have a positive duration and cannot exceed the 12-hour limit."""
    df = pyspark_obj.dataframe
    
    invalid_trips = df.filter(
        (F.col("dropoff_datetime") <= F.col("pickup_datetime")) | 
        ((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) > 12 * 3600) |
        F.col("dropoff_datetime").isNull() |
        F.col("pickup_datetime").isNull()
    )
    return invalid_trips.isEmpty()


@register_check_method
def valid_no_charge_fare(pyspark_obj: PysparkDataframeColumnObject) -> bool:
    """Trips flagged as 'No charge' must not have a generated metered fare amount."""
    df = pyspark_obj.dataframe
    invalid_trips = df.filter((F.col("payment_type_id") == 3) & (F.col("fare_amount") > 0.0))
    return invalid_trips.isEmpty()


@register_check_method
def valid_negotiated_fare(pyspark_obj: PysparkDataframeColumnObject) -> bool:
    """Trips recording 0.0 miles but charging a fare must be a Negotiated Fare."""
    df = pyspark_obj.dataframe
    invalid_trips = df.filter((F.col("ratecode_id") != 5) & (F.col("trip_distance_mi") == 0.0) & (F.col("fare_amount") > 0.0))
    return invalid_trips.isEmpty()

TLC_PYSPARK_VALIDATION_SCHEMA = pa.DataFrameSchema(
    columns={
        "vendor_id": pa.Column(T.ByteType, pa.Check.isin(list(VENDOR_MAPPING.keys())), nullable=False), 
        "pickup_datetime": pa.Column(T.TimestampType, nullable=False),
        "dropoff_datetime": pa.Column(T.TimestampType, nullable=False),
        "passenger_count": pa.Column(T.ByteType, pa.Check.in_range(0, 6), nullable=False),
        "trip_distance_mi": pa.Column(T.DoubleType, pa.Check.greater_than_or_equal_to(0.0), nullable=False),
        "ratecode_id": pa.Column(T.ByteType, pa.Check.isin(list(RATECODE_MAPPING.keys())), nullable=False),
        "fare_amount": pa.Column(T.DoubleType, nullable=False),
        "payment_type_id": pa.Column(T.ByteType, pa.Check.isin(list(PAYMENT_TYPE_MAPPING.keys())), nullable=False, required=False),
        "total_amount": pa.Column(T.DoubleType, nullable=False)
    },
    checks=[
        # Because we registered the methods, they are now available as native methods on pa.Check
        pa.Check.not_empty(
            error="The dataset is completely empty."
        ),
        pa.Check.valid_trip_duration(
            error="Trips must have a positive duration and cannot exceed the 12-hour TLC legal limit."
        ),
        pa.Check.valid_no_charge_fare(
            error="Trips flagged as 'No charge' must not have a generated metered fare amount."
        ),
        pa.Check.valid_negotiated_fare(
            error=(
                "Trips recording 0.0 miles but charging a fare indicate a defective taximeter, and it must be a Negotiated Fare. "
                "Any exception must be considered a TLC violation and investigated accordingly."
            )
        )
    ]
)