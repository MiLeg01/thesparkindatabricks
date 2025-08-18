# Databricks notebook source
# MAGIC %md
# MAGIC # **Databricks Training - Modul 2: Advanced DataFrame Operations & UDFs**
# MAGIC
# MAGIC Dieses Notebook deckt die folgenden Themen ab :
# MAGIC
# MAGIC 1. Datenexploration & Filterung
# MAGIC 2. Joins
# MAGIC 3. Transformationen: Select, WithColumn, Ausdrücke
# MAGIC 4. Gruppierungen & Aggregationen
# MAGIC 5. Einführung in UDFs (Benutzerdefinierte Funktionen)
# MAGIC 6. UDF mit mehreren Spalten
# MAGIC 7. Performance-Hinweise und Best Practices für UDFs
# MAGIC 8. Pandas_UDFs (Benutzerdefinierte Funktionen)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Setup und Dataset laden

# COMMAND ----------

# DBFS Pfad
DATA_PATH = "workspace.default.yellow_tripdata_2025_01" # "/FileStore/tables/yellow_tripdata_2025_01-1.parquet"

# DataFrame laden
df = spark.read.table(DATA_PATH)

# Schema
df.printSchema()

# Sample zeigen
display(df)

# COMMAND ----------

#dbutils.fs.ls("/Volumes/rdp_atz_rbgooe_landing/training/testdata/")
#df = spark.read.csv("/Volumes/rdp_atz_rbgooe_landing/training/testdata/testfile.csv")
#df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2. Datenexploration & Filterung

# COMMAND ----------

# Row Count
df.count()

# Statistik
#df.describe().show()
df.describe("trip_distance").show()

# Filter 
long_trips = df.filter(df.trip_distance > 10)
short_long_trips = df.filter((df.trip_distance > 10) | (df.trip_distance < 1))

display(long_trips)



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3. Datenexploration & Filterung

# COMMAND ----------

# Modul 2: Fortgeschrittene DataFrame-Operationen
# Kapitel: Joins - Grundlegende Einführung

# --------------------------------------
# Setup
# --------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek

spark = SparkSession.builder \
    .appName("NYC Yellow Taxi - Joins") \
    .getOrCreate()

# Dataset laden (angepasster Pfad je nach Umgebung)
taxi_df = spark.read.option("header", True).parquet("/data/yellow_tripdata_2025_01.parquet")

# Beispiel: kleine Tabelle mit NYC Borough Zuordnung
borough_data = [
    (1, "Manhattan"),
    (2, "Bronx"),
    (3, "Brooklyn"),
    (4, "Queens"),
    (5, "Staten Island")
]
borough_df = spark.createDataFrame(borough_data, ["LocationID", "Borough"])

# Annahme: taxi_df hat Pickup- und Dropoff-IDs

# --------------------------------------
# 1. Inner Join
# --------------------------------------
# Nur Fahrten, die eine passende Borough-Zuordnung haben
inner_join_df = taxi_df.join(
    borough_df,
    taxi_df["PULocationID"] == borough_df["LocationID"],
    "inner"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 2. Left Join
# --------------------------------------
# Alle Fahrten behalten, auch wenn keine Borough-Zuordnung existiert
left_join_df = taxi_df.join(
    borough_df,
    taxi_df["PULocationID"] == borough_df["LocationID"],
    "left"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 3. Right Join
# --------------------------------------
# Alle Boroughs behalten, auch wenn keine Fahrten existieren
right_join_df = taxi_df.join(
    borough_df,
    taxi_df["PULocationID"] == borough_df["LocationID"],
    "right"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 4. Full Outer Join
# --------------------------------------
# Alle Fahrten und alle Boroughs behalten
full_outer_join_df = taxi_df.join(
    borough_df,
    taxi_df["PULocationID"] == borough_df["LocationID"],
    "outer"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 5. Semi-Join
# --------------------------------------
semi_join_df = taxi_df.join(
    borough_df,
    taxi_df["PULocationID"] == borough_df["LocationID"],
    "left_semi"
)

# --------------------------------------
# 6. Anti-Join
# --------------------------------------
anti_join_df = taxi_df.join(
    borough_df,
    taxi_df["PULocationID"] == borough_df["LocationID"],
    "left_anti"
)

# --------------------------------------
# 7. Cross-Join
# --------------------------------------
weekday_df = taxi_df.select(dayofweek("tpep_pickup_datetime").alias("weekday")).distinct()
cross_join_df = borough_df.crossJoin(weekday_df)

# --------------------------------------
# Ergebnisse inspizieren
# --------------------------------------
print("Inner Join Beispiel:")
inner_join_df.show(5)

print("Left Join Beispiel:")
left_join_df.show(5)

print("Right Join Beispiel:")
right_join_df.show(5)

print("Full Outer Join Beispiel:")
full_outer_join_df.show(5)

print("Semi-Join Beispiel:")
semi_join_df.show(5)

print("Anti-Join Beispiel:")
anti_join_df.show(5)

print("Cross-Join Beispiel:")
cross_join_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4. Transformationen: Select, WithColumn, Ausdrücke

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Auswahl relevanter Spalten und Berechnung der Fahrtdauer
df_transformed = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)").alias("trip_duration_seconds")
)

display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5. Gruppierungen & Aggregationen

# COMMAND ----------

from pyspark.sql.functions import avg, count

# Durchschnittliche Entfernung und Fahrpreis pro Tag
df_gruppiert = df.groupBy(expr("date(tpep_pickup_datetime)").alias("fahrt_datum")) \
    .agg(
        avg("trip_distance").alias("durchschnitt_entfernung"),
        avg("fare_amount").alias("durchschnitt_fahrpreis"),
        count("*").alias("anzahl_fahrten")
    ) \
    .orderBy("fahrt_datum")

display(df_gruppiert)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6. Einführung in UDFs (Benutzerdefinierte Funktionen)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Beispiel: UDF zur Klassifikation der Fahrten basierend auf Entfernung
def fahrten_kategorie(entfernung):
    if entfernung < 1:
        return "Kurz"
    elif entfernung < 5:
        return "Mittel"
    else:
        return "Lang"

# UDF registrieren
kategorie_udf = udf(fahrten_kategorie, StringType())

# UDF anwenden
df_mit_kategorie = df.withColumn("fahrt_kategorie", kategorie_udf(col("trip_distance")))

display(df_mit_kategorie.select("trip_distance", "fahrt_kategorie"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7. UDF mit mehreren Spalten

# COMMAND ----------

from pyspark.sql.types import BooleanType

# Beispiel: Markiere verdächtige Fahrten mit langer Strecke, aber niedrigem Preis
def verdaechtige_fahrt(fahrpreis, entfernung):
    if entfernung > 5 and fahrpreis < 10:
        return True
    return False

# UDF registrieren
verdaechtige_udf = udf(verdaechtige_fahrt, BooleanType())

# UDF anwenden
df_flagged = df.withColumn("verdaechtig", verdaechtige_udf(col("fare_amount"), col("trip_distance")))

# Nur verdächtige Fahrten anzeigen
df_verdaechtig = df_flagged.filter(col("verdaechtig") == True)

display(df_verdaechtig)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8. Performance-Hinweise und Best Practices für UDFs
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 Wann soll ich UDFs verwenden?
# MAGIC
# MAGIC UDFs (User Defined Functions) sind benutzerdefinierte Funktionen, mit denen du komplexe Logik einbauen kannst, die nicht durch Spark-eigene Funktionen abgedeckt wird.
# MAGIC
# MAGIC Aber Achtung: Sie haben oft **Leistungsnachteile** gegenüber nativen Spark-Funktionen.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ✅ Verwende UDFs, wenn ...
# MAGIC
# MAGIC - du **komplexe Logik** brauchst, die mit Spark SQL-Funktionen **nicht möglich oder extrem unübersichtlich** wäre.
# MAGIC - du eine **Domänenlogik** abbildest (z. B. Klassifikation mit vielen Bedingungen oder Mapping-Tabellen).
# MAGIC - du Funktionen aus bestehenden Python-Codebasen wiederverwenden möchtest (z. B. medizinische Berechnungen, benutzerdefinierte Regeln).
# MAGIC - du einfache Datenbereinigungen automatisieren willst (z. B. Parsing, Kürzen, Maskieren, benutzerdefinierte Validierung).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ❌ Vermeide UDFs, wenn ...
# MAGIC
# MAGIC - es eine **eingebaute Spark-Funktion** gibt – die sind **deutlich schneller**, da sie in der Spark Engine optimiert ausgeführt werden (in Scala/Java).
# MAGIC   - Beispiele: `when`, `regexp_extract`, `substring`, `coalesce`, `datediff`, `lower`, `upper`, `concat`, `array_contains`, etc.
# MAGIC - du **große Datenmengen** verarbeitest und Performance ein zentrales Thema ist.
# MAGIC - du **komplexe Transformationen** auf numerischen Spalten durchführen willst – da sind `Spark SQL`, `Pandas UDFs` oder `Vectorized UDFs` wesentlich effizienter.
# MAGIC - du **Skalierbarkeit** willst – UDFs blockieren manchmal Optimierungen wie Predicate Pushdown oder Tungsten Code Generation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 💡 Bessere Alternativen prüfen:
# MAGIC
# MAGIC | Ziel                                 | Bessere Alternative             |
# MAGIC |--------------------------------------|---------------------------------|
# MAGIC | Einfache Berechnungen / Bedingungen | Spark SQL-Funktionen (`when`, `expr`) |
# MAGIC | Mapping / Transformation            | `withColumn`, `selectExpr`, `sql()` |
# MAGIC | Komplexe Analytik                   | Pandas UDFs oder Spark SQL |
# MAGIC | Textverarbeitung                    | `regexp_extract`, `split`, `instr` |
# MAGIC | Gruppierungen / Statistiken         | `groupBy().agg(...)` |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9. Pandas_UDFs (Benutzerdefinierte Funktionen)

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType
import pandas as pd

#Pandas_UDF definieren
@pandas_udf(StringType())
def fahrten_kategorie_pandas(entfernungen: pd.Series) -> pd.Series:
    ergebnisse = []
    for e in entfernungen:
        if e < 1:
            ergebnisse.append("Kurz")
        elif e < 5:
            ergebnisse.append("Mittel")
        else:
            ergebnisse.append("Lang")
    return pd.Series(ergebnisse)

# Pandas_UDF anwenden
df_mit_kategorie = df.withColumn(
    "fahrt_kategorie",
    fahrten_kategorie_pandas(col("trip_distance"))
)

df_mit_kategorie.select("trip_distance", "fahrt_kategorie").show()
