# Databricks notebook source
# MAGIC %md
# MAGIC #Modul 5 - Part 1: Structured Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.1. Setup und Dataset laden

# COMMAND ----------

# MAGIC %run "./Helper/_config"

# COMMAND ----------

STREAMING_INPUT_FOLDER = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/jsonfolder"

try:
    spark.sql(f"DROP VOLUME {CATALOG}.{SCHEMA}.taxi_streaming_output;")
except Exception as e:
    print("Volume konnte nicht gelöscht werden")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.taxi_streaming_output;")

STREAMING_OUTPUT_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_streaming_output"

print(STREAMING_OUTPUT_VOLUME)

dbutils.fs.mkdirs(f"{STREAMING_OUTPUT_VOLUME}/streamingdata")
dbutils.fs.mkdirs(f"{STREAMING_OUTPUT_VOLUME}/checkpointdir")

STREAMING_OUTPUT_FOLDER = f"{STREAMING_OUTPUT_VOLUME}/streamingdata"
STREAMING_CHECKPOINT = f"{STREAMING_OUTPUT_VOLUME}/checkpointdir"


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2. Streaming Quelle erzeugen

# COMMAND ----------

# MAGIC %md
# MAGIC Gehe zu Notebook Helper/Modul 5 - Streaming Input

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.3. Stream einlesen

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define schema (important for streaming)
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("taxi_id", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("pickup_datetime", StringType(), True),
    StructField("dropoff_datetime", StringType(), True)
])

# Create streaming DataFrame
df_stream = spark.readStream \
    .schema(schema) \
    .json(STREAMING_INPUT_FOLDER)


# COMMAND ----------

""" table version
streaming_df = (
    spark.readStream
         .table(input_path)
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4. Stream verarbeiten durch Logik

# COMMAND ----------

from pyspark.sql.functions import avg, count

agg_df = (
    df_stream
        .groupBy("passenger_count")
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare")
        )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ##5.5. Stream schreiben in Zieltabelle 

# COMMAND ----------

query = (
    agg_df.writeStream
         .queryName("stream_demo")   
         .outputMode("complete")              # replace results on each trigger
         .option("checkpointLocation", STREAMING_CHECKPOINT)
         .format("delta")
         .trigger(availableNow=True)
         .start(STREAMING_OUTPUT_FOLDER)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM delta.`/Volumes/workspace/default/taxi_streaming_output/streamingdata`
