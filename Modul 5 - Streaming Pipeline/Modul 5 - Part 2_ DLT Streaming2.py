# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ##Modul 5 - Part 2: DLT Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6. DLT Pipeline definiert

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_timestamp, count, avg, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

CATALOG = spark.conf.get("CATALOG")
SCHEMA = spark.conf.get("SCHEMA")

STREAMING_INPUT_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/jsonfolder"

# Define schema
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("taxi_id", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True)
])

# Raw streaming view
@dlt.view(comment="Raw streaming trips data")
def raw_trips_table():
    return (
        spark.readStream
             .schema(schema)
             .format("json")
             .load(STREAMING_INPUT_FOLDER)
             .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
    )

# Hourly aggregation table
@dlt.table(comment="Aggregated trip count and average fare per passenger count per hour")
def trips_by_passenger_count_hourly():
    return (
        dlt.read_stream("raw_trips_table")
           # Wait up to 1 hour for late data
           .withWatermark("pickup_datetime", "1 hour")
           .groupBy(
               window(col("pickup_datetime"), "1 hour"),  # 1-hour window
               col("passenger_count")
           )
           .agg(
               count("*").alias("trip_count"),
               avg("fare_amount").alias("avg_fare")
           )
           # Flatten the window columns for easier querying
           .select(
               col("window.start").alias("window_start"),
               col("window.end").alias("window_end"),
               col("passenger_count"),
               col("trip_count"),
               col("avg_fare")
           )
    )

