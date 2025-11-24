# Databricks notebook source
# MAGIC %md
# MAGIC #Modul 4: In- und Output Operationen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1. Setup und Dataset laden

# COMMAND ----------

# MAGIC %run "./Helper/_config"

# COMMAND ----------

# DBFS Pfad
DATA_PATH = f"{CATALOG}.{SCHEMA}.yellow_tripdata_2025_01"
LOOKUP_PATH =  f"{CATALOG}.{SCHEMA}.taxi_zone_lookup"

# DataFrame laden
df_taxi = spark.read.table(DATA_PATH)
df_lookup = spark.read.table(LOOKUP_PATH)

try:
    spark.sql(f"DROP VOLUME {CATALOG}.{SCHEMA}.taxi_volume;")
except Exception as e:
    print("Volume konnte nicht gelöscht werden")
    
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.taxi_volume;")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/taxi_volume/"

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.2. Speichern im JSON Format

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2.1 Speichern

# COMMAND ----------

# Taxi-Daten als JSON speichern
df_taxi.write.mode("overwrite").json(f"{VOLUME_PATH}/taxi_json")

# Lookup-Daten als JSON speichern
df_lookup.write.mode("overwrite").json(f"{VOLUME_PATH}/lookup_json")


# COMMAND ----------

# MAGIC %md
# MAGIC ###4.2.2. Einlesen

# COMMAND ----------

df_taxi_json = spark.read.json(f"{VOLUME_PATH}/taxi_json")
df_lookup_json = spark.read.json(f"{VOLUME_PATH}/lookup_json")

df_taxi_json.show(5)
df_lookup_json.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ##4.3. Speicherung im CSV Format

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3.1. Speichern

# COMMAND ----------

# Taxi-Daten als CSV speichern
df_taxi.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/taxi_csv")

# Lookup-Daten als CSV speichern
df_lookup.write.mode("overwrite").option("header", True).csv(f"{VOLUME_PATH}/lookup_csv")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3.2. Auslesen

# COMMAND ----------

df_taxi_csv = spark.read.option("header", True).csv(f"{VOLUME_PATH}/taxi_csv")
df_lookup_csv = spark.read.option("header", True).csv(f"{VOLUME_PATH}/lookup_csv")

df_taxi_csv.show(5)
df_lookup_csv.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4. Speicherung im Parquet-Format

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4.1. Speichern

# COMMAND ----------

# Taxi-Daten als Parquet im Managed Volume speichern
df_taxi.write.mode("overwrite").parquet(f"{VOLUME_PATH}/taxi_parquet")

# Lookup-Daten als Parquet speichern
df_lookup.write.mode("overwrite").parquet(f"{VOLUME_PATH}/lookup_parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4.2. Auslesen

# COMMAND ----------

df_taxi_parquet = spark.read.parquet(f"{VOLUME_PATH}/taxi_parquet")
df_lookup_parquet = spark.read.parquet(f"{VOLUME_PATH}/lookup_parquet")

#df_taxi_parquet.show(5)
display(df_taxi_parquet)

df_lookup_parquet.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ##4.5. Speichern im Delta Format

# COMMAND ----------

# MAGIC %md
# MAGIC ###4.5.1. Speichern

# COMMAND ----------

# Taxi-Daten als Delta speichern
df_taxi.write.format("delta").mode("overwrite").save(f"{VOLUME_PATH}/taxi_delta")

# Lookup-Daten als Delta speichern
df_lookup.write.format("delta").mode("overwrite").save(f"{VOLUME_PATH}/lookup_delta")


# COMMAND ----------

# MAGIC %md
# MAGIC ###4.5.2. Auslesen

# COMMAND ----------

df_taxi_delta = spark.read.format("delta").load(f"{VOLUME_PATH}/taxi_delta")
df_lookup_delta = spark.read.format("delta").load(f"{VOLUME_PATH}/lookup_delta")

df_taxi_delta.show(5)
df_lookup_delta.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ##4.4.3 Time Travel

# COMMAND ----------

from pyspark.sql.functions import col

df_taxi_delta_km = df_taxi_delta.withColumn("trip_distance", col("trip_distance") * 1.60934)


# COMMAND ----------

df_taxi_delta_km.write.format("delta").mode("overwrite").save(f"{VOLUME_PATH}/taxi_delta")


# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{VOLUME_PATH}/taxi_delta`"))

df_taxi_delta_0 = spark.read.format("delta").option("versionAsOf", 0).load(f"{VOLUME_PATH}/taxi_delta")
display(df_taxi_delta_0)

df_taxi_delta_1 = spark.read.format("delta").option("versionAsOf", 1).load(f"{VOLUME_PATH}/taxi_delta")
display(df_taxi_delta_1)

#Frage: Was passiert, wenn ich ohne versionAsOf Lade?

#ToDO: Laden über timestampAsOf
