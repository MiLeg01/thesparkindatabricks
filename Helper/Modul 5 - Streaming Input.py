# Databricks notebook source
# MAGIC %run "./_config"

# COMMAND ----------

# DBFS Pfad
DATA_PATH = f"{CATALOG}.{SCHEMA}.yellow_tripdata_2025_01" # "/FileStore/tables/yellow_tripdata_2025_01-1.parquet"
LOOKUP_PATH = f"{CATALOG}.{SCHEMA}.taxi_zone_lookup"

# DataFrame laden
df_taxi = spark.read.table(DATA_PATH)
df_lookup = spark.read.table(LOOKUP_PATH)

try:
    spark.sql(f"DROP VOLUME {CATALOG}.{SCHEMA}.taxi_volume;")
except Exception as e:
    print("Volume konnte nicht gelöscht werden")
    
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.taxi_volume;")

# COMMAND ----------

import json
import uuid
import time
import os
from pyspark.sql.functions import year, col

# Zielvolume
streaming_input_volume = f"Volumes/{CATALOG}/{SCHEMA}/taxi_volume"
streaming_input_folder = f"{streaming_input_volume}/jsonfolder"

# Delta Pfad

dbutils.fs.mkdirs(f"/{streaming_input_volume}/jsonfolder")

timestamp_col = "tpep_pickup_datetime" 

df_taxi_small = (
    df_taxi
    .filter(year(col(timestamp_col)) == 2025)
    .orderBy(timestamp_col) 
    .limit(10000)
)

for i, row in enumerate(df_taxi_small.collect()):
    # Row in dict umwandeln, Datum als ISO
    row_dict = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in row.asDict().items() }
    
    # Dateiname im DBFS-Pfad
    file_path = f"/{streaming_input_folder}/event_{i:06d}.json"
    
    # JSON schreiben
    dbutils.fs.put(file_path, json.dumps(row_dict), overwrite=True)
    
    print(f"Wrote event {i+1}")
    time.sleep(3)  # Pause für Streaming-Simulation

