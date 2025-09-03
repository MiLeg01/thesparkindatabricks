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

#input_path = "workspace.streaming_input.inputtable"
STREAMING_INPUT_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/jsonfolder"

# Read from your source table
@dlt.table(
    name="raw_trips",
    comment="Raw streaming trips data"
)
def raw_trips():
    return (
        spark.readStream
             .format("json")
             .load(STREAMING_INPUT_FOLDER)
    )

# Aggregate trips by passenger_count
@dlt.table(
    name="trips_by_passenger_count",
    comment="Aggregated trip count and average fare by passenger count"
)
def trips_by_passenger_count():
    return (
        dlt.read("raw_trips")
           .groupBy("passenger_count")
           .agg(
               count("*").alias("trip_count"),
               avg("fare_amount").alias("avg_fare")
           )
    )
