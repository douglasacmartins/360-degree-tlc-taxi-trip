"""
NYC TLC Trip Record Schema and Data Dictionary Configuration.

This module defines the canonical schema, data cleaning rules, categorical
mappings, and metadata descriptions for unifying the New York City Taxi and 
Limousine Commission (TLC) Trip Record datasets (Yellow/TPEP and Green/LPEP).

It provides centralized, immutable data structures designed to standardize 
disparate taxi data formats into a single, cohesive ETL pipeline.

Key Components:
    - Type Aliases: Semantic types for column names, identifiers, and descriptions 
      to improve code readability and static analysis.
    - Schema Definitions: Sets defining the canonical columns to retain (`COMMON_COLUMNS`) 
      and deprecated/unused columns to remove (`COLUMNS_TO_DROP`).
    - Transformation Mappings: Rules for standardizing dataset-specific nomenclature 
      into a unified format (`RENAME_MAPPING`).
    - Categorical Decoders: Immutable dictionaries mapping numerical IDs to human-readable 
      string values (e.g., `RATECODE_MAPPING`, `VENDOR_MAPPING`, `PAYMENT_TYPE_MAPPING`).
    - Data Dictionary: A comprehensive metadata catalog (`DESCRIPTIONS_MAPPING`) providing 
      detailed descriptions for every canonical column, dynamically enriched with 
      categorical value mappings.
"""

from types import MappingProxyType
from typing import Annotated, Final

SnakeCaseColumnName = Annotated[str, "Snake case columns names"]
CanonicalColumnName = Annotated[str, "Canonical names for columns"]
ColumnDescription = Annotated[str, "Description for columns"]
Id = Annotated[int, "Identifier for categorical variables"]


COMMON_COLUMNS: Final[set[CanonicalColumnName]] = {
    'congestion_surcharge',
    'dropoff_datetime',
    'dropoff_location_id',
    'extra',
    'fare_amount',
    'improvement_surcharge',
    'mta_tax',
    'passenger_count',
    'payment_type_id',
    'pickup_datetime',
    'pickup_location_id',
    'ratecode_id',
    'store_and_foward',
    'tip_amount',
    'tolls_amount',
    'total_amount',
    'trip_distance_mi',
    'vendor_id'
}

# NOTE: There is no data dictionary neither values to those columns
COLUMNS_TO_DROP: Final[set[str]] = {
    "ehail_fee"
}

RENAME_MAPPING: Final[MappingProxyType[
    SnakeCaseColumnName,
    CanonicalColumnName
]] = MappingProxyType({
    "lpep_pickup_datetime": "pickup_datetime",
    "tpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "store_and_fwd_flag": "store_and_foward",
    "pu_location_id": "pickup_location_id",
    "do_location_id": "dropoff_location_id",
    "trip_distance": "trip_distance_mi",
    "payment_type": "payment_type_id",
    "trip_type": "trip_type_id"
})

REVERSE_RENAME_MAPPING: Final[dict[CanonicalColumnName, list[SnakeCaseColumnName]]] = {
    v: [k for k, val in RENAME_MAPPING.items() if val == v] 
    for v in set(RENAME_MAPPING.values())
}

RATECODE_MAPPING: Final[MappingProxyType[Id, str]] = MappingProxyType({
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Null/unknown"
})

VENDOR_MAPPING: Final[MappingProxyType[Id, str]] = MappingProxyType({
    1: "Creative Mobile Technologies, LLC",
    2: "Curb Mobility, LLC",
    6: "Myle Technologies Inc",
    7: "Helix"
})

PAYMENT_TYPE_MAPPING: Final[MappingProxyType[Id, str]] = MappingProxyType({
    0: "Flex Fare trip",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
})

TRIP_TYPE_MAPPING: Final[MappingProxyType[Id, str]] = MappingProxyType({
    1: "Street-hail",
    2: "Dispatch"
})

ORIGIN_TYPE_MAPPING: Final[MappingProxyType[Id, str]] = MappingProxyType({
    1: "TPEP",
    2: "LPEP"
})

TAXI_ZONE_LOOKUP_DESCRIPTIONS_MAPPING: Final[MappingProxyType[SnakeCaseColumnName, str]] = MappingProxyType({
    "location_id": (
        "A numeric identifier (1-265) for the TLC Taxi Zone, used as the primary key to join with the PULocationID and DOLocationID fields in the trip records."
    ),
    
    "borough": (
        "The broad geographic region of the taxi zone, representing one of the five NYC boroughs or a specific out-of-city hub like Newark Airport (EWR)"
    ),
    
    "zone": (
        "The specific neighborhood name associated with the taxi zone, which roughly approximates the NYC Department of City Planning’s Neighborhood Tabulation Areas (NTAs)"
    ),
    
    "service_zone": (
        "The TLC operational jurisdiction of the zone. Key values include \"Yellow Zone\" (the Hail Exclusionary Zone where only Yellow Taxis can accept street hails), \"Boro Zone\" (the Hail Zone where Green Taxis are permitted to accept street hails), and \"EWR\" (Newark Airport)"
    )
})

DESCRIPTIONS_MAPPING: Final[MappingProxyType[CanonicalColumnName, ColumnDescription]] = MappingProxyType({
    "origin": (
        "The source of the trip record, either 'LPEP' for Green Taxis or 'TPEP' for Yellow Taxis. "
        "This prefix is used in the original dataset to distinguish between the two taxi types, "
        "but is dropped in the canonical schema for simplicity and consistency across both datasets."
    ),

    "origin_id": (
        "Trip's source dataset identifier. "
        "This field is derived from the original dataset's filename prefix and serves as a standardized identifier for the taxi type in the unified schema."
        '\n'.join([f'{k} = {v}' for k, v in ORIGIN_TYPE_MAPPING.items()])
    ),
    
    "vendor": (
        "Name of the technology provider (TSP) that dispatched the record. "
        "Shared by both Green and Yellow taxis."
    ),

    "vendor_id": (
        "A code indicating the technology provider (TSP) that dispatched the record. "
        "Shared by both Green and Yellow taxis."
        '\n'.join([f'{k} = {v}' for k, v in VENDOR_MAPPING.items()])
    ),

    "pickup_datetime": (
        "The exact date and time the trip started and the meter was engaged."
    ),
    
    "dropoff_datetime": (
        "The exact date and time the trip ended and the meter was disengaged. "
        "Must be strictly after the pickup time; identical times indicate a 0-duration anomaly such as a canceled trip."
    ),
    
    "store_and_foward": (
        "A 'Y' or 'N' flag indicating if the vehicle temporarily lost server connection, "
        "requiring trip data to be saved locally in the vehicle's memory before transmitting. "
        "A null value represent a manual inserted trip."
    ),
    
    "ratecode": (
        "Final rate applied to the trip (e.g., Standard, JFK, Newark)."
    ),

    "ratecode_id": (
        "The categorical code for the final rate applied to the trip (e.g., Standard, JFK, Newark). "
        "Code 99 indicates a null or unknown rate."
        '\n'.join([f'{k} = {v}' for k, v in RATECODE_MAPPING.items()])
    ),
    
    "pickup_location_id": (
        "The TLC Taxi Zone ID (ranging from 1-265) where the trip started and the taximeter was engaged. "
        "Can be mapped to specific boroughs and neighborhoods using an external Taxi Zone lookup table."
    ),
    
    "dropoff_location_id": (
        "The TLC Taxi Zone ID (ranging from 1-265) where the trip ended and the taximeter was disengaged."
    ),
    
    "passenger_count": (
        "The number of passengers in the vehicle, typically entered manually by the driver. "
        "Legal TLC capacity is a maximum of 4 or 5 adults depending on the vehicle, plus one child seated on an adult's lap."
    ),
    
    "trip_distance_mi": (
        "The total elapsed trip distance in miles, as calculated and reported by the vehicle's taximeter."
    ),
    
    "fare_amount": (
        "The core time-and-distance fare calculated by the meter, excluding taxes, tips, and surcharges."
    ),
    
    "mta_tax": (
        "A standard $0.50 tax automatically triggered by the meter for trips ending in NYC or surrounding counties "
        "(e.g., Nassau, Westchester) to fund the Metropolitan Transportation Authority."
    ),
    
    "tip_amount": (
        "Tips paid automatically via credit card. Cash tips are completely excluded from this dataset."
    ),
    
    "tolls_amount": (
        "The total sum of any bridge or tunnel tolls paid during the trip."
    ),
    
    "improvement_surcharge": (
        "A fixed surcharge added at the flag drop (start of the trip), enacted in 2015 to fund accessible vehicles."
    ), 
    
    "total_amount": (
        "The final total amount charged to the passenger, excluding cash tips. "
        "Zero or negative values are anomalies usually indicating a voided or disputed trip."
    ),

    "payment_type_id": (
        "Identifier for the payment method used for the trip."
    ),

    "payment_type": (
        "Payment method. Methods like Dispute and 6 Voided trip help identify financial anomalies."
        '\n'.join([f'{k} = {v}' for k, v in PAYMENT_TYPE_MAPPING.items()])
    ),
    
    "trip_type_id": (
        "Identifier for the trip type, indicating how the trip was hailed. "
    ),

    "trip_type": (
        "Exclusive to Green Taxis (LPEP). Indicates if the trip was hailed on the street (1) or pre-arranged via dispatch (2)."
        '\n'.join([f'{k} = {v}' for k, v in TRIP_TYPE_MAPPING.items()])
    ),
    
    "congestion_surcharge": (
        "Total amount collected for the New York State congestion surcharge, applied to trips entering heavily congested areas."
    ),

    "cbd_congestion_fee": (
        "A specific per-trip fee designed to fund the MTA's Congestion Relief Zone, effective January 5, 2025."
    ),

    "airport_fee": (
        "Exclusive to Yellow Taxis (TPEP). An automatic fee applied only for pickups at LaGuardia (LGA) or John F. Kennedy (JFK) Airports."
    ),

    "extra": (
        "A catch-all field for any additional charges or fees not categorized elsewhere. "
    ),

    "ehail_fee": (
        "Exclusive to Green Taxis (LPEP). A fee applied to trips hailed via the e-hail app, which was discontinued in September 21, 2024. "
        "This field may contain null values for trips after the discontinuation date."
    ),

    "year": (
        "The year of the trip pickup, derived from the pickup_datetime date field."
    ),

    "month": (
        "The month of the trip pickup, derived from the pickup_datetime date field."
    ),

    # Taxi Zone lookup table descriptions, prefixed to indicate their source in the original dataset
    **{suffix+k: v for k, v in TAXI_ZONE_LOOKUP_DESCRIPTIONS_MAPPING.items() for suffix in ["pickup_", "dropoff_"]}
})