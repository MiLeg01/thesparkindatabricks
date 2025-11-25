# Databricks notebook source
# MAGIC %md
# MAGIC # Modul 6 - Testing und Logging

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.1. Setup und Dataset laden

# COMMAND ----------

# MAGIC %run "./Helper/_config"

# COMMAND ----------

# Daten zuerst in DBFS hochladen
DATA_PATH = f"{CATALOG}.{SCHEMA}.yellow_tripdata_2025_01"
LOOKUP_PATH = f"{CATALOG}.{SCHEMA}.taxi_zone_lookup"

# DataFrame laden
df_taxi = spark.read.table(DATA_PATH)
df_lookup = spark.read.table(LOOKUP_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ##6.2. Testing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2.1. Unit Testing in PySpark
# MAGIC
# MAGIC Unit Testing ist ein essenzieller Bestandteil der Softwareentwicklung. Ziel ist es, kleine, isolierte Teile des Codes - sogenannte "Units" - auf korrekte Funktionalität zu prüfen. In PySpark-Workflows bedeutet das häufig, DataFrame-Transformationen, UDFs oder Business-Logik zu testen.
# MAGIC
# MAGIC Inhalt:
# MAGIC 1. Welche Python-Pakete für Unit Tests verwendet werden.
# MAGIC 2. Wie sie sich im Kontext von PySpark unterscheiden.
# MAGIC 3. Vor- und Nachteile der jeweiligen Pakete.
# MAGIC 4. Beispielhafte Anwendung auf PySpark DataFrames.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2.2. Beliebte Python-Pakete für Unit Testing
# MAGIC
# MAGIC In Python gibt es mehrere Frameworks für Unit Testing. Die gängigsten sind:
# MAGIC
# MAGIC 1. **unittest**
# MAGIC 2. **pytest**
# MAGIC 3. **nose2**
# MAGIC
# MAGIC Wir vergleichen diese im Kontext von PySpark.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### unittest
# MAGIC
# MAGIC - **Beschreibung:** Standardmodul von Python, benötigt keine zusätzliche Installation.
# MAGIC - **Stärken:**
# MAGIC   - Kommt mit Python vorinstalliert
# MAGIC   - Unterstützt Test Suites, Setup/Teardown-Methoden
# MAGIC   - Gut dokumentiert
# MAGIC - **Schwächen:**
# MAGIC   - Syntax ist etwas umständlich
# MAGIC   - Flexibilität im Vergleich zu pytest eingeschränkt
# MAGIC - **Einsatz in PySpark:**
# MAGIC   - Gut für strukturierte, kleine Tests
# MAGIC   - Tests von DataFrame-Transformationen sind möglich, aber Assertions müssen manuell gemacht werden (z. B. `assert df.collect() == expected_data`) - Achtung auf Lazy Evaluation hier
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### pytest
# MAGIC
# MAGIC - **Beschreibung:** Sehr populäres, flexibles Test-Framework
# MAGIC - **Stärken:**
# MAGIC   - Sehr einfache und klare Syntax
# MAGIC   - Unterstützt Fixtures für Setup/Teardown
# MAGIC   - Umfangreiche Plugins (z. B. `pytest-spark` für Spark-spezifische Tests)
# MAGIC   - Assertions sind lesbar und informativ
# MAGIC - **Schwächen:**
# MAGIC   - Externe Installation nötig (`pip install pytest`)
# MAGIC - **Einsatz in PySpark:**
# MAGIC   - Ideal für DataFrame-Tests
# MAGIC   - Mit `assert df.collect() == expected_data` oder `df.schema == expected_schema`
# MAGIC   - Kann parametrische Tests für verschiedene Szenarien erstellen
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### nose2
# MAGIC
# MAGIC - **Beschreibung:** Weiterentwicklung des älteren `nose`-Frameworks
# MAGIC - **Stärken:**
# MAGIC   - Automatische Test-Discovery
# MAGIC   - Plugins verfügbar
# MAGIC - **Schwächen:**
# MAGIC   - Weniger populär als `pytest`, weniger Community-Support
# MAGIC   - Entwicklung nicht so aktiv
# MAGIC - **Einsatz in PySpark:**
# MAGIC   - Funktioniert ähnlich wie unittest
# MAGIC   - Eher weniger verbreitet für Spark-Projekte
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vergleich der Unit Testing Frameworks
# MAGIC
# MAGIC | Framework | Vorteile | Nachteile | PySpark Eignung |
# MAGIC |-----------|----------|-----------|----------------|
# MAGIC | unittest  | Standardmodul, keine Installation nötig, solide | Verbose Syntax, weniger flexibel | Gut für kleine Tests, manuelles Assertion Handling |
# MAGIC | pytest    | Einfache Syntax, Fixtures, Plugins, parametrische Tests | Externe Installation nötig | Sehr gut geeignet, lesbare DataFrame-Assertions |
# MAGIC | nose2     | Automatische Test-Discovery, Plugins | Weniger populär, Entwicklung nicht aktiv | Mittel, eher selten verwendet |
# MAGIC
# MAGIC ### Empfehlung für PySpark-Projekte
# MAGIC
# MAGIC Für Databricks-PySpark-Projekte wird **pytest** oft empfohlen:
# MAGIC
# MAGIC - Einfacher Umgang mit DataFrames
# MAGIC - Leicht in CI/CD-Pipelines integrierbar
# MAGIC - Umfangreiche Community- und Plugin-Unterstützung
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2.3. Praxisteil mit pytest

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

#Schema Tests definieren
import pytest

def test_tripdata_schema():
    df = df_taxi
    expected_fields = {
        "VendorID": "string",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "double",
        "trip_distance": "double",
        "RatecodeID": "double",
        "store_and_fwd_flag": "string",
        "PULocationID": "integer",
        "DOLocationID": "integer",
        "payment_type": "double",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double"
    }
    for field in df.schema.fields:
        assert field.name in expected_fields
        assert field.dataType.simpleString() == expected_fields[field.name]

def test_zone_lookup_schema():
    df = df_lookup
    expected_fields = {"LocationID": "int", "Borough": "string", "Zone": "string", "service_zone": "string"}
    for field in df.schema.fields:
        assert field.name in expected_fields
        assert field.dataType.simpleString() == expected_fields[field.name]

# COMMAND ----------

## Schema Tests ausführen

!pytest -v --assert=plain


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.3. Spark UI

# COMMAND ----------

#Beispielhafte Query zur Analyse:
long_trips = df_taxi.filter(df_taxi.trip_distance > 10)
long_trips.show()

short_trips = df_taxi.filter(df_taxi.trip_distance < 1)
short_trips.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/michael.legenstein@hotmail.com/thesparkindatabricks/Helper/Images/spark-query-analyse.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.4. Logging

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Bedeutung von Logging
# MAGIC
# MAGIC Logging ist ein essenzielles Werkzeug in der Datenverarbeitung, besonders in großen Systemen wie Spark. Es hilft dabei:
# MAGIC
# MAGIC - Den Fortschritt von Jobs zu überwachen
# MAGIC - Fehler frühzeitig zu erkennen und zu debuggen
# MAGIC - Informationen über Datenverarbeitungsschritte zu dokumentieren
# MAGIC - Performance-Probleme oder Engpässe zu identifizieren
# MAGIC
# MAGIC ### 2. Logging Levels
# MAGIC
# MAGIC In Python/PySpark werden Logs üblicherweise nach Schweregrad kategorisiert:
# MAGIC
# MAGIC - **DEBUG**: Detaillierte Informationen für Entwickler. Wird selten in Produktion verwendet.
# MAGIC - **INFO**: Allgemeine Informationen über den Ablauf eines Jobs.
# MAGIC - **WARNING**: Hinweise auf potenzielle Probleme, die aber den Job nicht stoppen.
# MAGIC - **ERROR**: Fehler, die einen Job oder einen Prozess stoppen oder fehlschlagen lassen.
# MAGIC - **CRITICAL**: Sehr schwere Fehler, die sofortige Aufmerksamkeit erfordern.
# MAGIC
# MAGIC ### 3. Logging in PySpark
# MAGIC
# MAGIC In Spark gibt es zwei Ebenen für Logging:
# MAGIC
# MAGIC 1. **Spark-eigenes Logging**: Wird über `log4j` oder Spark UI konfiguriert und zeigt Infos über Jobs, Stages und Tasks.
# MAGIC 2. **Custom Logging**: Eigene Python-Logs mit dem `logging` Modul. Ideal für Zwischenergebnisse, Data Quality Checks oder Warnungen während Transformationen.
# MAGIC
# MAGIC ### 4. Best Practices
# MAGIC
# MAGIC - Logs sollten aussagekräftig und prägnant sein.
# MAGIC - Verwende unterschiedliche Levels, um Wichtiges von Routine-Informationen zu unterscheiden.
# MAGIC - Achte darauf, dass große DataFrames nicht komplett in den Logs ausgegeben werden – nur Summaries oder Samples.
# MAGIC - Nutze Logs für Monitoring in produktiven Pipelines.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3.1. Konsolen Logger

# COMMAND ----------

import logging

# Logger konfigurieren
logger = logging.getLogger("TaxiLogger")
logger.setLevel(logging.INFO)

# Konsole als Ausgabe
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logger.info("Logging konfiguriert.")


# COMMAND ----------

# Lade Daten mit Logging
logger.info("Lade Taxi Trips Dataset...")
try:
    df_taxi = spark.read.table(DATA_PATH)
    logger.info(f"Taxi Trips Dataset erfolgreich geladen. Anzahl Zeilen: {df_taxi.count()}")
except Exception as e:
    logger.error(f"Fehler beim Laden des Trips-Datasets: {e}")

logger.info("Lade Taxi Zone Lookup Dataset...")
try:
    df_lookup = spark.read.table(LOOKUP_PATH)
    logger.info(f"Zone Lookup Dataset erfolgreich geladen. Anzahl Zeilen: {df_taxi.count()}")
except Exception as e:
    logger.error(f"Fehler beim Laden des Zone-Datasets: {e}")

# COMMAND ----------

from pyspark.sql.functions import col, count

logger.info("Beginne Aggregation nach Abholzone...")

try:
    trips_by_pickup_zone = df_taxi.groupBy("PULocationID").agg(count("*").alias("trip_count"))
    logger.info("Aggregation erfolgreich. Beispiel-Ergebnisse:")
    trips_by_pickup_zone.show(5)
except Exception as e:
    logger.error(f"Fehler während der Aggregation: {e}")

# COMMAND ----------

logger.info("Führe Join mit Zone Lookup durch...")

try:
    trips_with_zone = trips_by_pickup_zone.join(df_lookup, trips_by_pickup_zone.PULocationID == df_lookup.LocationID, "left")
    logger.info("Join erfolgreich. Beispiel-Ergebnisse:")
    trips_with_zone.select("PULocationID", "zone", "trip_count").show(5)
except Exception as e:
    logger.error(f"Fehler beim Join: {e}")


# COMMAND ----------

logger.info("Überprüfe auf Zonen mit weniger als 10 Trips...")
few_trips = trips_with_zone.filter(col("trip_count") < 10).collect()

if few_trips:
    logger.warning(f"Es gibt {len(few_trips)} Zonen mit weniger als 10 Trips!")
else:
    logger.info("Alle Zonen haben mindestens 10 Trips.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3.2. File Logger

# COMMAND ----------

from datetime import datetime

# Liste zum Sammeln von Logs
log_entries = []

def log_to_table(level, message):
    """Speichert Log-Messages zusätzlich in einer Liste für spätere Persistenz"""
    log_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "message": message
    }
    log_entries.append(log_entry)
    # Zusätzlich auch in die normale Console loggen
    if level == "INFO":
        logger.info(message)
    elif level == "WARNING":
        logger.warning(message)
    elif level == "ERROR":
        logger.error(message)

# COMMAND ----------

log_to_table("INFO", "Starte Aggregation nach Abholzone...")

try:
    trips_by_pickup_zone = df_taxi.groupBy("PULocationID").agg(count("*").alias("trip_count"))
    log_to_table("INFO", "Aggregation erfolgreich durchgeführt.")
    trips_by_pickup_zone.show(5)
except Exception as e:
    log_to_table("ERROR", f"Fehler während der Aggregation: {e}")

try:
    trips_by_pickup_zone = df_tax.groupBy("PULocationID").agg(count("*").alias("trip_count"))
    log_to_table("INFO", "Aggregation erfolgreich durchgeführt.")
    trips_by_pickup_zone.show(5)
except Exception as e:
    log_to_table("ERROR", f"Fehler während der Aggregation: {e}")


# COMMAND ----------

# Logs in Spark DataFrame konvertieren
logs_df = spark.createDataFrame(log_entries)

display(logs_df)  # Zeigt die Logs direkt im Notebook an
logs_df.write.mode("append").format("delta").saveAsTable("taxi_logs")
log_to_table("INFO", "Logs wurden erfolgreich in der Tabelle 'taxi_logs' gespeichert.")

# COMMAND ----------

# Übung:
# Logging Dashboard bauen
