# Databricks notebook source
# MAGIC %md
# MAGIC # Modul 3: Performance-Optimierung in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC Dieses Notebook deckt die folgenden Themen ab :
# MAGIC
# MAGIC 1. Lazy Evaluation & Ausführungspläne (`explain`)
# MAGIC 2. Caching & Persistierung
# MAGIC 3. Partitionierung & Auswirkungen (inkl. Partition Pruning)
# MAGIC 4. Daten-Skew & Salting-Techniken
# MAGIC 5. Broadcast-Variablen & Akkumulatoren
# MAGIC 6. Spark SQL: Catalyst- & Tungsten-Optimierungen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1. Setup und Dataset laden

# COMMAND ----------

# %pip install pyarrow pandas  # In Databricks i.d.R. bereits vorhanden
import os

from pyspark.sql.functions import col, lit, year, month, when, rand, broadcast, monotonically_increasing_id, expr
from pyspark.sql import functions as F
from pyspark import StorageLevel
import time

#spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Passen Sie die Shuffle-Partitionen an die Clustergröße an
spark.conf.set("spark.sql.shuffle.partitions", "200")

DATA_PATH = "workspace.default.yellow_tripdata_2025_01"
LOOKUP_PATH = "workspace.default.taxi_zone_lookup"
#WORK_BASE = "Workspace:/Users/michael.legenstein@accenture.com/thesparkindatabricks/tmp/nyc_perf_modul3"
#dbutils.fs.mkdirs(WORK_BASE)
df_taxi = spark.read.table(DATA_PATH)
df_lookup = spark.read.table(LOOKUP_PATH)

# Write to local temp
path = "/tmp/nyc_perf_modul3"
os.makedirs(f"file:{path}", exist_ok=True)

#df_taxi.write.mode("overwrite").parquet(f"file:{path}/data.parquet")

print("Spark Version:", spark.version)

# COMMAND ----------

print(os.listdir('file:'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2. Lazy Evaluation & Ausführungspläne verstehen
# MAGIC
# MAGIC Spark führt Transformationen nicht sofort aus. Erst eine *Action* (z. B. `count`, `show`, `collect`, `write`) startet die Berechnung.
# MAGIC Nutze `DataFrame.explain(...)` um Logical/Optimized/Physical Plan einzusehen.

# COMMAND ----------

# Mehrere Transformationen – noch keine Ausführung
df_long = df_taxi.filter(col("trip_distance") > 5).select("PULocationID", "DOLocationID", "trip_distance")

print("Noch keine Ausführung erfolgt (Lazy). Jetzt explain:")
df_long.explain("extended")  # zeigt Logical, Optimized, Physical Plan

print("\nJetzt Action via count():")
t0 = time.time()
n = df_long.count()
t1 = time.time()
print("Zeilen:", n, "  Dauer (s):", round(t1 - t0, 2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3. Caching- & Persistenzstrategien
# MAGIC
# MAGIC Wenn ein DataFrame mehrfach verwendet wird, lohnt sich `cache()` bzw. `persist()`.

# COMMAND ----------

df_jan = df_taxi.filter((col("tpep_pickup_datetime") >= "2016-01-01") & (col("tpep_pickup_datetime") < "2016-02-01"))

# Ohne Cache: zwei aufeinanderfolgende Actions
t0 = time.time(); _ = df_jan.select("trip_distance").agg(F.avg("trip_distance")).collect(); t1 = time.time()
t2 = time.time(); _ = df_jan.select("fare_amount").agg(F.avg("fare_amount")).collect(); t3 = time.time()
print(f"Ohne Cache: avg(dist) {round(t1-t0,2)}s, avg(fare) {round(t3-t2,2)}s")

# Mit Cache
df_jan_cached = df_jan.cache()
_ = df_jan_cached.count()  # Materialisierung

t4 = time.time(); _ = df_jan_cached.select("trip_distance").agg(F.avg("trip_distance")).collect(); t5 = time.time()
t6 = time.time(); _ = df_jan_cached.select("fare_amount").agg(F.avg("fare_amount")).collect(); t7 = time.time()
print(f"Mit Cache:   avg(dist) {round(t5-t4,2)}s, avg(fare) {round(t7-t6,2)}s")

df_jan_cached.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4. Partitionierung & Auswirkungen
# MAGIC
# MAGIC - `repartition(n)` verteilt Daten neu (Shuffle), gut für Parallelisierung.
# MAGIC - `coalesce(n)` reduziert Partitionen ohne vollen Shuffle.
# MAGIC - **Partition Pruning**: Beim Schreiben nach `year`, `month` partitionieren und beim Lesen gezielt filtern.

# COMMAND ----------

print("Aktuelle Anzahl Partitionen:", df_taxi.rdd.getNumPartitions())

# Beispiel Repartition/Coalesce (Demo, nicht zwingend ausführen)
demo = df_taxi.select("tpep_pickup_datetime", "trip_distance")
demo = demo.repartition(64)
print("Nach repartition(64):", demo.rdd.getNumPartitions())
demo = demo.coalesce(16)
print("Nach coalesce(16):", demo.rdd.getNumPartitions())

# Partitioniert schreiben (kleine Teilmenge, um Demo schnell zu halten)
small = df_taxi.withColumn("year", year("tpep_pickup_datetime")).withColumn("month", month("tpep_pickup_datetime"))           .filter((col("year")==2016) & (col("month").isin(1,2)))
out_path = f"{WORK_BASE}/yellow_partitioned"

# idempotent säubern
dbutils.fs.rm(out_path, recurse=True)
small.write.mode("overwrite").partitionBy("year","month").parquet(out_path)

print("Partitioniertes Dataset geschrieben nach:", out_path)

# Partition Pruning zeigen
df_part = spark.read.parquet(out_path)
print("Explain ohne Filter:")
df_part.select("trip_distance").explain("formatted")

print("\nExplain mit Filter (year=2016, month=1):")
df_pruned = df_part.filter((col("year")==2016) & (col("month")==1))
df_pruned.select("trip_distance").explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5. Daten-Skew & Salting-Techniken
# MAGIC
# MAGIC Wenn wenige Keys extrem viele Zeilen besitzen, können einzelne Tasks viel länger laufen.
# MAGIC **Salting** verteilt Hot-Keys auf mehrere Partitionen.

# COMMAND ----------

from time import time

# Künstlichen Skew-Key erzeugen: 70% der Zeilen auf einen einzigen Key
skew_df = df_taxi.select("PULocationID", "trip_distance")\
            .withColumn("hot", when(rand() < 0.7, lit(1)).otherwise(lit(0)))\
            .withColumn("join_key", when(col("hot")==1, lit(999)).otherwise(col("PULocationID")))
skew_df = skew_df.unionAll(skew_df).unionAll(skew_df).unionAll(skew_df)

# Eine kleine Dimensionstabelle zum Join (distinct Keys)
dim = df_taxi.select(col("PULocationID").alias("join_key")).distinct()         .withColumn("factor", lit(1.23))

# --- Schlechter (skewed) Join
joined_skew = skew_df.join(dim, on="join_key", how="inner")
#print("Skewed Join Plan:")
#joined_skew.explain("formatted")

# --- Skewed Join (ohne Salting)
start = time()
skew_count = joined_skew.count()   # Aktion -> Job wird wirklich ausgeführt
end = time()
print(f"Skewed Join Count: {skew_count}, Dauer: {end - start:.2f} Sekunden")

# --- Salting (verteilt Hot-Key auf 10 'Eimer')
salt_buckets = 10
skew_salted = skew_df.withColumn("salt", (rand()*salt_buckets).cast("int"))
#skew_salted.show()

dim_salted = dim.crossJoin(spark.range(salt_buckets).withColumnRenamed("id","salt"))
#dim_salted.show()

joined_salted = skew_salted.join(dim_salted, on=["join_key","salt"], how="inner")
#print("\nSalted Join Plan:")
#joined_salted.explain("formatted")

start = time()
salted_count = joined_salted.count()
end = time()
print(f"Salted Join Count: {salted_count}, Dauer: {end - start:.2f} Sekunden")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6. Broadcast-Variablen & Akkumulatoren
# MAGIC
# MAGIC ### Broadcast-Join (DataFrame API)
# MAGIC Kleine Lookup-Tabellen werden an alle Worker gebroadcastet, um Shuffles zu vermeiden.
# MAGIC
# MAGIC ### RDD Broadcast & Accumulator
# MAGIC Für seiteneffektreiche Aufgaben (Logging, Zähler) in `foreach`/RDD-Operationen.

# COMMAND ----------

# Broadcast-Join mit Taxi-Zonen-Lookup
lookup_df = spark.read.table(LOOKUP_PATH)\
                 .withColumnRenamed("LocationID","PULocationID")
df_bcast = df_taxi.join(broadcast(lookup_df), on="PULocationID", how="left")

print("Broadcast-Join Plan:")
df_bcast.explain("formatted")

# --- SparkContext Broadcast & Accumulator (RDD/foreach)
bc_threshold = spark.sparkContext.broadcast(1.0)  # z.B. Kurzstrecken-Schwelle
acc_short = spark.sparkContext.accumulator(0)

def count_short(row):
    global acc_short
    if row.trip_distance is not None and row.trip_distance < bc_threshold.value:
        acc_short += 1

# Achtung: foreach löst Job aus; hier auf kleiner Teilmenge demonstrieren
df_taxi.limit(500000).select("trip_distance").foreach(count_short)
print("Gezählte Kurzstrecken (Sample):", acc_short.value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7. Spark SQL: Catalyst & Tungsten
# MAGIC
# MAGIC - **Catalyst** optimiert Logik: Predicate Pushdown, Column Pruning, Join-Reorder, Constant Folding u.v.m.
# MAGIC - **Tungsten** beschleunigt Ausführung: Off-Heap Memory, Cache-effiziente Datenformate, Whole-Stage Codegen.
# MAGIC
# MAGIC ### Demos
# MAGIC - Vergleiche DataFrame-API vs. SQL – der Optimizer erzeugt denselben Ausführungsplan.
# MAGIC - Zeige *codegen* (generierter Java-Code) und *formatted* Plan.

# COMMAND ----------

# DataFrame-Variante
df_dfapi = df_taxi.filter((col("trip_distance") > 2) & (col("fare_amount") > 5))\
             .groupBy("PULocationID")\
             .agg(F.avg("trip_distance")\
             .alias("avg_dist"))

print("DataFrame-Plan (formatted):")
df_dfapi.explain("formatted")

# SQL-Variante (soll gleichwertigen Plan erzeugen)
df_taxi.createOrReplaceTempView("trips")
sql_df_taxi = spark.sql('''
  SELECT PULocationID, AVG(trip_distance) AS avg_dist
  FROM trips
  WHERE trip_distance > 2 AND fare_amount > 5
  GROUP BY PULocationID
''')

print("\nSQL-Plan (formatted):")
sql_df_taxi.explain("formatted")

# Codegen anzeigen (Ausschnitt)
print("\nDataFrame-Plan (codegen):")
df_dfapi.explain("codegen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Zusammenfassung / Checkliste
# MAGIC
# MAGIC - **Lazy**: Plane Pipelines, triggere gezielt Actions.
# MAGIC - **Cache**: Wenn mehrfach genutzt – danach `unpersist()`.
# MAGIC - **Partitionen**: Anzahl & Größe anpassen; beim Schreiben sinnvoll partitionieren → Pruning.
# MAGIC - **Skew**: Erkennen in der Spark UI; bei Bedarf **Salting**, ggf. `skewHint` oder adaptive execution.
# MAGIC - **Broadcast**: Kleine Dimensionen broadcasten; Akkus/Broadcast für RDD/foreach-Szenarien.
# MAGIC - **Catalyst/Tungsten**: Nutze `explain(...)` (formatted/cost/codegen), um Optimierungen zu verstehen.
