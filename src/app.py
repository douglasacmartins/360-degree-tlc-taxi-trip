import glob
import pathlib
from typing import Final

import pyspark.sql.functions as F
import pyspark.sql.types as T
from data_pipeline import run_pipeline
from pyspark.sql import SparkSession
from string_utils import camel_to_snake
from tlc_schema import TAXI_ZONE_LOOKUP_DESCRIPTIONS_MAPPING

DATA_PATH: Final[pathlib.Path] = pathlib.Path(__file__).parent.parent / "data"
print(f"Data path: {DATA_PATH}")
spark = (
    SparkSession.builder
    .appName("tlc_pipeline")
    .getOrCreate()
)
taxi_zone_lookup = spark.read.csv(str(DATA_PATH / "taxi_zone_lookup.csv"), header=True)
taxi_zone_lookup = taxi_zone_lookup.select([F.col(c).alias(camel_to_snake(c)) for c in taxi_zone_lookup.columns])
taxi_zone_lookup = (
    taxi_zone_lookup
    .withColumn("location_id", F.col("location_id").cast(T.IntegerType()))
    .withColumn("borough", F.col("borough").cast(T.StringType()))
    .withColumn("zone", F.col("zone").cast(T.StringType()))
    .withColumn("service_zone", F.col("service_zone").cast(T.StringType()))
)
for col_name in taxi_zone_lookup.columns:
    description = TAXI_ZONE_LOOKUP_DESCRIPTIONS_MAPPING.get(camel_to_snake(col_name), None)
    if description:
        taxi_zone_lookup = taxi_zone_lookup.withColumn(
            col_name, 
            F.col(col_name).alias(col_name, metadata={"comment": description})
        )
    
files = glob.glob(str(DATA_PATH / "*" / "*_tripdata_2023*.parquet"))

df = run_pipeline(spark, files, zone_lookup=taxi_zone_lookup)
df = df.withColumn("year", F.year("pickup_datetime")).withColumn("month", F.month("pickup_datetime"))
df = df.repartition("year", "month")

# Attach data dictionary as the absolute last step to ensure no metadata gets stripped by transformations
from data_pipeline import attach_data_dictionary

df = attach_data_dictionary(df)

df.printSchema()
df.write.drop("year", "month").partitionBy("year", "month").mode("overwrite").parquet("/app/output/silver")
spark.stop()