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
# MAGIC   - Syntax ist etwas verbose
# MAGIC   - Flexibilität im Vergleich zu pytest eingeschränkt
# MAGIC - **Einsatz in PySpark:**
# MAGIC   - Gut für strukturierte, kleine Tests
# MAGIC   - Tests von DataFrame-Transformationen sind möglich, aber Assertions müssen manuell gemacht werden (z. B. `assert df.collect() == expected_data`)
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

#Schema Tests definieren

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

!pytest


# COMMAND ----------

## Schema Tests ausführen

!pytest


# COMMAND ----------

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

def test_tripdata_schema(test_df: DataFrame, test_schema: dict):

    for field in test_df.schema.fields:
        assert field.name in test_schema
        assert field.dataType.simpleString() == test_schema[field.name]

expected_fields_taxi = {
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

expected_fields_lookup = {
    "LocationID": "int", 
    "Borough": "string", 
    "Zone": "string", 
    "service_zone": "string"
    }

