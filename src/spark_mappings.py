from types import MappingProxyType
from typing import Final, Type

import pyspark.sql.types as T

CASTING_MAPPING: Final[MappingProxyType[str, T.DataType]] = MappingProxyType({
    "vendor_id": T.ByteType(),
    "payment_type_id": T.ByteType(),
    "trip_type_id": T.ByteType(),
    "ratecode_id": T.ByteType(),
    "passenger_count": T.ByteType(),
    "pickup_location_id": T.ShortType(),
    "dropoff_location_id": T.ShortType(),
    "store_and_foward": T.StringType(),
    "pickup_datetime": T.TimestampType(),
    "dropoff_datetime": T.TimestampType(),
})