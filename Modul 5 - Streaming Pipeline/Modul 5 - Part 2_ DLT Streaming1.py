# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ##Modul 5 - Part 2: DLT Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6. DLT Pipeline definiert

# COMMAND ----------

import dlt
from pyspark.sql.functions import avg, count, to_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

CATALOG = spark.conf.get("CATALOG")
SCHEMA = spark.conf.get("SCHEMA")

#input_path = "workspace.streaming_input.inputtable"
STREAMING_INPUT_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/jsonfolder"

schema = StructType([
    StructField("VendorID", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True)
])

# Read from your source table
@dlt.view(
    comment="Raw streaming trips data"
)
def raw_trips_table():
    return (
        spark.readStream
             .schema(schema)
             .format("json")
             .load(STREAMING_INPUT_FOLDER)
             .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
             .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
    )

# Aggregate trips by passenger_count
@dlt.table(
    comment="Aggregated trip count and average fare by passenger count"
)
def trips_by_passenger_count():
    return (
        dlt.readStream("raw_trips_table")
           .withWatermark("pickup_datetime", "10 minutes")
           .groupBy("passenger_count")
           .agg(
               count("*").alias("trip_count"),
               avg("fare_amount").alias("avg_fare")
           )
    )

