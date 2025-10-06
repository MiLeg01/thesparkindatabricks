# Databricks notebook source
# MAGIC %md
# MAGIC #Modul 2: Advanced DataFrame Operations & UDFs
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Dieses Notebook deckt die folgenden Themen ab :
# MAGIC
# MAGIC 1. Datenexploration & Filterung
# MAGIC 2. Transformationen: Select, WithColumn, Ausdrücke
# MAGIC 3. Gruppierungen & Aggregationen
# MAGIC 4. Window Functions
# MAGIC 5. Joins
# MAGIC 6. Einführung in UDFs (Benutzerdefinierte Funktionen)
# MAGIC 7. UDF mit mehreren Spalten
# MAGIC 8. Performance-Hinweise und Best Practices für UDFs
# MAGIC 9. Pandas_UDFs (Benutzerdefinierte Funktionen)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1. Setup und Dataset laden

# COMMAND ----------

# MAGIC %run "./Helper/_config"

# COMMAND ----------

#Das sind die Daten mit denen wir arbeiten + Erklärung:
#https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
#https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# DBFS Pfad
DATA_PATH = f"{CATALOG}.{SCHEMA}.yellow_tripdata_2025_01"
LOOKUP_PATH = f"{CATALOG}.{SCHEMA}.taxi_zone_lookup"

# DataFrame laden
df_taxi = spark.read.table(DATA_PATH)
df_lookup = spark.read.table(LOOKUP_PATH)

# Schema
df_taxi.printSchema()
df_lookup.printSchema()

# Sample zeigen
df_taxi.show(5)
display(df_taxi.limit(5))

df_lookup.show(5)
display(df_lookup.limit(5))

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
display(short_long_trips)

# COMMAND ----------

### YOUR CODE HERE ###
# df_trips.filter( ... ).select( ... ).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3. Transformationen: Select, WithColumn, Ausdrücke

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

#Berechnung der Durchschnittsgeschwindigkeit

### YOUR CODE HERE ###
# df_trips = df_trips.withColumn("...", ...)
# df_trips.select("...", "...").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4. Gruppierungen & Aggregationen

# COMMAND ----------

from pyspark.sql.functions import avg, count, expr

# Durchschnittliche Entfernung und Fahrpreis pro Tag
df_gruppiert = df_taxi.groupBy(expr("date(tpep_pickup_datetime)").alias("fahrt_datum")) \
    .agg(
        avg("trip_distance").alias("durchschnitt_entfernung"),
        avg("fare_amount").alias("durchschnitt_fahrpreis"),
        count("*").alias("anzahl_fahrten")
    ) \
    .orderBy("fahrt_datum")

df_gruppiert.explain("extended")
#display(df_gruppiert)


# COMMAND ----------

#Berechnung der obigen Werte auf Monatsbasis

### YOUR CODE HERE ###
#df_gruppiert_monat = df_gruppiert.groupBy() \
#    .agg(
#    ) \
#    .orderBy("fahrt_datum")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5. Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, to_date, col

#Pro Tag die drei Fahrten mit der größten trip_distance und zeigt deren Abholzeit, Distanz und Rang an.
w = Window.partitionBy(to_date("tpep_pickup_datetime")).orderBy(desc("trip_distance"))

df_ranked = df_taxi.withColumn("rank", rank().over(w))
df_ranked.filter(col("rank") <= 3) \
    .select("tpep_pickup_datetime", "trip_distance", "rank") \
    .show(10)


# COMMAND ----------

#Höchste Rate pro Tag

### YOUR CODE HERE ###
# w = Window.partitionBy(to_date("...")).orderBy(desc("..."))
# df_ranked = df_trips.withColumn("rank", rank().over(w))
# df_ranked.filter(col("rank") <= 3).show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6. Joins

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
# MAGIC ## 2.7. Einführung in UDFs (Benutzerdefinierte Funktionen)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Beispiel: UDF zur Klassifikation der Fahrten basierend auf Entfernung
def fahrten_kategorie(entfernung: int):
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

from pyspark.sql.functions import when, col
 
df_mit_kategorie = df_taxi.withColumn(
    "fahrt_kategorie",
    when(col("trip_distance") < 1, "Kurz")
    .when(col("trip_distance") < 5, "Mittel")
    .otherwise("Lang")
)
 
display(
    df_mit_kategorie.select("trip_distance", "fahrt_kategorie")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8. UDF mit mehreren Spalten

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
# MAGIC ## 2.9. Performance-Hinweise und Best Practices für UDFs
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wann soll ich UDFs verwenden?
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
# MAGIC ## 2.10. Pandas_UDFs (Benutzerdefinierte Funktionen)

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

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StringType
import pandas as pd

# Schreibe eine Pandas UDF, die aus der tip_amount und der fare_amount eine Trinkgeld-Kategorie bestimmt:
# "Keine" → wenn tip_amount == 0
# "Normal" → wenn tip_amount/fare_amount < 0.2
# "Großzügig" → wenn tip_amount/fare_amount >= 0.2

# Definiere hier deine eigene Pandas UDF
@pandas_udf(StringType())
def tip_kategorie_pandas(tips: pd.Series, fares: pd.Series) -> pd.Series:
    ergebnisse = []
    for t, f in zip(tips, fares):
        ### YOUR CODE HERE ###
        # if ...
        # elif ...
        # else ...
        pass
    return pd.Series(ergebnisse)

# Wende deine Pandas UDF auf den DataFrame an
df_mit_tip_kategorie = df_taxi.withColumn(
    "tip_kategorie",
    tip_kategorie_pandas(col("tip_amount"), col("fare_amount"))
)

df_mit_tip_kategorie.select("fare_amount", "tip_amount", "tip_kategorie").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.11. Stores Procedures

# COMMAND ----------

# MAGIC %md
# MAGIC Stored Procedures sind wiederverwendbare Bausteine, die Logik in einer Datenbank kapseln – vergleichbar mit Funktionen in Programmiersprachen.  
# MAGIC Sie bieten folgende Vorteile:
# MAGIC
# MAGIC - **Wiederverwendbarkeit:** Einmal geschrieben, können sie mehrfach ausgeführt werden.  
# MAGIC - **Wartbarkeit:** Änderungen an der Logik erfolgen zentral.  
# MAGIC - **Sicherheit:** Zugriff auf sensible Operationen kann über Berechtigungen auf Prozeduren statt auf Tabellen gesteuert werden.  
# MAGIC - **Automatisierung:** Häufig wiederkehrende Analysen oder Transformationsschritte können einfach automatisiert werden.  
# MAGIC
# MAGIC In Databricks lassen sich Stored Procedures mit **SQL** definieren und sowohl mit SQL- als auch mit PySpark-Workloads kombinieren.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table-Valued Functions (TVFs) vs. Stored Procedures – Theorie
# MAGIC
# MAGIC Sowohl **TVFs** (Table-Valued Functions) als auch **Stored Procedures** können Tabellen zurückgeben, unterscheiden sich aber deutlich in **Zweck, Funktionsweise und Einsatz**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Direktvergleich: TVFs vs. Stored Procedures
# MAGIC
# MAGIC | Thema | **Table-Valued Functions (TVFs)** | **Stored Procedures** |
# MAGIC |-------|------------------------------------|------------------------|
# MAGIC | **Definition** | Parametrisierte Views: wiederverwendbare SQL-Abfrage, die immer eine Tabelle zurückgibt. | Ein gespeichertes Programm in der Datenbank, das mehrere SQL-Befehle und Logik kapselt. |
# MAGIC | **Rückgabe** | Immer genau eine Tabelle (ähnlich einer View). | Kann eine Tabelle, einzelne Werte oder gar nichts zurückgeben. |
# MAGIC | **Aufruf** | Wird wie eine Tabelle in einem `SELECT` genutzt: `SELECT * FROM f(parameter)`. | Mit `CALL procedure(parameter)` aufgerufen. |
# MAGIC | **Komplexität** | Einfach: nur eine Abfrage, keine Mehrschritt-Logik. | Sehr flexibel: mehrere SQL-Statements, Kontrollstrukturen, dynamisches SQL. |
# MAGIC | **Seiteneffekte** | Keine Datenänderungen möglich. | Kann Daten verändern (INSERT, UPDATE, DELETE). |
# MAGIC | **Performance** | Optimierbar wie Views; Query Planner kann TVFs gut optimieren. | Schwerer optimierbar, da komplexe Logik enthalten sein kann. |
# MAGIC | **Typischer Einsatz** | Wiederverwendbare Filter- oder Auswahlabfragen. | ETL-Prozesse, komplexe Transformationen, Automatisierungen. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Zusammenfassung
# MAGIC
# MAGIC - **TVFs**  
# MAGIC   - Funktioniert wie eine **parametrisierte View**.  
# MAGIC   - Gibt immer eine Tabelle zurück.  
# MAGIC   - Keine Seiteneffekte, deterministisch, leichtgewichtig.  
# MAGIC   - Ideal für wiederholte, parametrische Abfragen.  
# MAGIC
# MAGIC - **Stored Procedures**  
# MAGIC   - Funktioniert wie ein **SQL-Programm**.  
# MAGIC   - Kann Tabellen zurückgeben, Daten verändern oder Workflows steuern.  
# MAGIC   - Unterstützt komplexe Logik und mehrere Schritte.  
# MAGIC   - Ideal für ETL, Automatisierung und aggregierte Workflows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Merksatz:**  
# MAGIC - TVFs = **Views mit Parametern** → leichtgewichtig, nur SELECT.  
# MAGIC - Stored Procedures = **Programme in SQL** → mächtig, flexibel, Workflows & Datenänderungen möglich.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxi_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE PROCEDURE sp_avg_trip_stats_daily (IN day_of_month STRING)
# MAGIC LANGUAGE SQL
# MAGIC SQL SECURITY INVOKER
# MAGIC AS
# MAGIC BEGIN
# MAGIC   SELECT 
# MAGIC       date(t.tpep_pickup_datetime) AS trip_date,
# MAGIC       z.Zone AS pickup_zone,
# MAGIC       ROUND(AVG(unix_timestamp(t.tpep_dropoff_datetime) - unix_timestamp(t.tpep_pickup_datetime)) / 60, 2) AS avg_trip_minutes,
# MAGIC       ROUND(AVG(t.total_amount), 2) AS avg_total_amount,
# MAGIC       COUNT(*) AS trip_count
# MAGIC     FROM default.yellow_tripdata_2025_01 t
# MAGIC     JOIN default.taxi_zone_lookup z
# MAGIC       ON t.PULocationID = z.LocationID
# MAGIC     WHERE date(t.tpep_pickup_datetime) = day_of_month
# MAGIC     GROUP BY trip_date, z.Zone
# MAGIC     ORDER BY trip_date ASC, avg_total_amount DESC;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC Error! Lösung? **SQL Editor**

# COMMAND ----------

# MAGIC %sql
# MAGIC CALL workspace.taxi_analytics.sp_avg_trip_stats_daily(('2025-01-28'));

# COMMAND ----------

#Übung: 
# 
# Schreibe die Stored Procedure so um, dass die Daten persistiert werden. Es sollen dabei für einen Tag aber keine doppelten Einträge vorhanden sein. D.h. wenn es schon Daten für einen gegebenen Tag gibt, sollen die vorher gelöscht werden!
