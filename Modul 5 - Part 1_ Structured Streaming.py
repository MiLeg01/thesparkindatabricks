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
    StructField("VendorID", StringType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True)
])

# Create streaming DataFrame
input_stream = spark.readStream \
    .schema(schema) \
    .json(STREAMING_INPUT_FOLDER)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4. Stream verarbeiten durch Logik

# COMMAND ----------

from pyspark.sql.functions import avg, count

count_df = (
    input_stream
        .groupBy("passenger_count")
        .agg(
            count("*").alias("trip_count"),
            avg("fare_amount").alias("avg_fare")
        )
)


# COMMAND ----------

#ToDO: Aggregation über 1h Fenstern - Durchschnittliche Fahrpreise und Gesamtpreise, Anzahl Fahrten




# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##5.5. Stream schreiben in Zieltabelle 

# COMMAND ----------

query = (
    input_stream.writeStream
         .queryName("stream_demo")   
         #.outputMode("complete")              # replace results on each trigger, other is "update"
         .outputMode("append")
         .option("checkpointLocation", STREAMING_CHECKPOINT)
         .format("delta")
         .trigger(availableNow=True)
         .start(STREAMING_OUTPUT_FOLDER)
)


# COMMAND ----------

query = f"""
SELECT *
FROM delta.`/Volumes/{CATALOG}/{SCHEMA}/taxi_streaming_output/streamingdata`
"""

display(spark.sql(query))
