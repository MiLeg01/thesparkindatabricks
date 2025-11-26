# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ##Modul 5 - Part 2: DLT Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7. DLT Pipeline Übung

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, to_timestamp, count, avg, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

CATALOG = spark.conf.get("CATALOG")
SCHEMA = spark.conf.get("SCHEMA")

STREAMING_INPUT_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/jsonfolder"

# Define schema
schema = StructType([
    StructField("VendorID", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True)
])

# Raw streaming view
@dlt.table(comment="Raw streaming trips data")
def raw_trips_table_hourly():
    return (
        spark.readStream
             .schema(schema)
             .format("json")
             .load(STREAMING_INPUT_FOLDER)
             .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
    )

@dlt.table(comment="Aggregated trip count and average fare per passenger count per hour")
def trips_by_passenger_count_hourly():
    return (
        dlt.read_stream("raw_trips_table_hourly")
           .groupBy(
               window(col("pickup_datetime"), "1 hour"),
               col("passenger_count")
           )
           .agg(
               count("*").alias("trip_count"),
               avg("fare_amount").alias("avg_fare")
           )
           .select(
               col("window.start").alias("window_start"),
               col("window.end").alias("window_end"),
               col("passenger_count"),
               col("trip_count"),
               col("avg_fare")
           )
    )

