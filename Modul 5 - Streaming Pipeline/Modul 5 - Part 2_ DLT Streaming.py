# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ##Modul 5 - Part 2: DLT Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6. DLT Pipeline definiert

# COMMAND ----------

import dlt
from pyspark.sql.functions import avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

CATALOG = spark.conf.get("CATALOG")
SCHEMA = spark.conf.get("SCHEMA")

#input_path = "workspace.streaming_input.inputtable"
STREAMING_INPUT_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/jsonfolder"

schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("taxi_id", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True)
])

# Read from your source table
@dlt.table(
    comment="Raw streaming trips data"
)
def raw_trips_table():
    return (
        spark.readStream
             .schema(schema)
             .format("json")
             .load(STREAMING_INPUT_FOLDER)
    )

# Aggregate trips by passenger_count
@dlt.table(
    comment="Aggregated trip count and average fare by passenger count"
)
def trips_by_passenger_count():
    return (
        spark.read.table("raw_trips_table")
           .groupBy("passenger_count")
           .agg(
               count("*").alias("trip_count"),
               avg("fare_amount").alias("avg_fare")
           )
    )

