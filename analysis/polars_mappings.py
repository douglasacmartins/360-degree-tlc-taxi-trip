from types import MappingProxyType
from typing import Final, Type

import polars as pl

CASTING_MAPPING: Final[MappingProxyType[str, Type[pl.DataType]]] = MappingProxyType({
    "vendor_id": pl.Int8,
    "payment_type_id": pl.Int8,
    "trip_type_id": pl.Int8,
    "ratecode_id": pl.Int8,
    "passenger_count": pl.Int8,
    "pickup_location_id": pl.Int16,
    "dropoff_location_id": pl.Int16,
    "store_and_foward": pl.Categorical,
    "pickup_datetime": pl.Datetime,
    "dropoff_datetime": pl.Datetime,
})