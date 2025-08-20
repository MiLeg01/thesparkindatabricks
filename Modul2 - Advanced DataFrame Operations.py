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

#https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# DBFS Pfad
DATA_PATH = "workspace.default.yellow_tripdata_2025_01" # "/FileStore/tables/yellow_tripdata_2025_01-1.parquet"
LOOKUP_PATH = "workspace.default.df_lookup"

# DataFrame laden
df_taxi = spark.read.table(DATA_PATH)
df_lookup = spark.read.table(LOOKUP_PATH)

# Schema
df_taxi.printSchema()

# Sample zeigen
df_taxi.show(5)
df_lookup.show(5)

# COMMAND ----------

#dbutils.fs.ls("/Volumes/rdp_atz_rbgooe_landing/training/testdata/")
#df_taxi = spark.read.csv("/Volumes/rdp_atz_rbgooe_landing/training/testdata/testfile.csv")
#df_taxi.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2. Datenexploration & Filterung

# COMMAND ----------

# Row Count
df_taxi.count()

# Statistik
#df_taxi.describe().show()
df_taxi.describe("trip_distance").show()

# Filter 
long_trips = df_taxi.filter(df_taxi.trip_distance > 10)
short_long_trips = df_taxi.filter((df_taxi.trip_distance > 10) | (df_taxi.trip_distance < 1))

long_trips.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3. Joins

# COMMAND ----------


from pyspark.sql.functions import col, dayofweek

# --------------------------------------
# 1. Inner Join
# --------------------------------------
# Beispiel: Fahrten mit Borough-Namen aus dem Lookup
inner_join_df = df_taxi.join(
    df_lookup,
    df_taxi["PULocationID"] == df_lookup["LocationID"],
    "inner"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 2. Left Join
# --------------------------------------
left_join_df = df_taxi.join(
    df_lookup,
    df_taxi["PULocationID"] == df_lookup["LocationID"],
    "left"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 3. Right Join
# --------------------------------------
right_join_df = df_taxi.join(
    df_lookup,
    df_taxi["PULocationID"] == df_lookup["LocationID"],
    "right"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 4. Full Outer Join
# --------------------------------------
full_outer_join_df = df_taxi.join(
    df_lookup,
    df_taxi["PULocationID"] == df_lookup["LocationID"],
    "outer"
).select("*", col("Borough").alias("Pickup_Borough"))

# --------------------------------------
# 5. Semi-Join
# --------------------------------------
semi_join_df = df_taxi.join(
    df_lookup,
    df_taxi["PULocationID"] == df_lookup["LocationID"],
    "left_semi"
)

# --------------------------------------
# 6. Anti-Join
# --------------------------------------
anti_join_df = df_taxi.join(
    df_lookup,
    df_taxi["PULocationID"] == df_lookup["LocationID"],
    "left_anti"
)

# --------------------------------------
# 7. Cross-Join
# --------------------------------------
weekday_df = df_taxi.select(dayofweek("tpep_pickup_datetime").alias("weekday")).distinct()
cross_join_df = df_lookup.crossJoin(weekday_df)

# --------------------------------------
# Ergebnisse inspizieren
# --------------------------------------
print("Inner Join Beispiel:")
#print(inner_join_df.summary().toPandas())
print(inner_join_df.describe().toPandas())
#inner_join_df.show(15)

print("Left Join Beispiel:")
#print(inner_join_df.summary().toPandas())
print(inner_join_df.describe().toPandas())
#left_join_df.show(15)

print("Right Join Beispiel:")
#right_join_df.show(15)

print("Full Outer Join Beispiel:")
#full_outer_join_df.show(15)

print("Semi-Join Beispiel:")
#semi_join_df.show(15)

print("Anti-Join Beispiel:")
#anti_join_df.show(15)

print("Cross-Join Beispiel:")
#cross_join_df.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4. Transformationen: Select, WithColumn, Ausdrücke

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Auswahl relevanter Spalten und Berechnung der Fahrtdauer
df_transformed = df_taxi.select(
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
df_gruppiert = df_taxi.groupBy(expr("date(tpep_pickup_datetime)").alias("fahrt_datum")) \
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
df_mit_kategorie = df_taxi.withColumn("fahrt_kategorie", kategorie_udf(col("trip_distance")))

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
df_flagged = df_taxi.withColumn("verdaechtig", verdaechtige_udf(col("fare_amount"), col("trip_distance")))

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
df_mit_kategorie = df_taxi.withColumn(
    "fahrt_kategorie",
    fahrten_kategorie_pandas(col("trip_distance"))
)

df_mit_kategorie.select("trip_distance", "fahrt_kategorie").show()
