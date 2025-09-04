# Databricks notebook source
# MAGIC %md
# MAGIC # Modul1: Einführung in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Einführung in Apache Spark
# MAGIC Apache Spark ist ein Framework für **verteilte Datenverarbeitung**, das auf großen Clustern effizient arbeitet.
# MAGIC
# MAGIC Kernmerkmale:
# MAGIC
# MAGIC - **In-Memory-Verarbeitung:** Spark kann Daten im RAM halten, was die Verarbeitung im Vergleich zu diskbasierten Systemen stark beschleunigt.
# MAGIC - **Skalierbarkeit:** Spark läuft auf einem einzelnen Rechner, aber auch auf Hunderten von Nodes in einem Cluster.
# MAGIC - **Vielseitigkeit:** Spark unterstützt Batch-Processing, Streaming, SQL-Abfragen, Machine Learning (MLlib) und Graphverarbeitung (GraphX).
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1.1. Vorteile von Spark gegenüber klassischen Ansätzen
# MAGIC
# MAGIC | Vorteil                     | Beschreibung |
# MAGIC |-------------------------------|-------------|
# MAGIC | Geschwindigkeit               | Bis zu 100x schneller als klassische MapReduce-Ansätze, dank In-Memory-Verarbeitung |
# MAGIC | Skalierbarkeit                 | Verarbeitung von Terabytes oder Petabytes an Daten über Cluster hinweg |
# MAGIC | Flexibilität                   | Unterstützt unterschiedliche Datenformate (CSV, JSON, Parquet, Delta) und APIs (Python, Scala, Java, R) |
# MAGIC | Einheitliche Plattform         | Ein Framework für ETL, Analytics, Machine Learning und Streaming |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1.2. Cluster-Architektur von Spark
# MAGIC
# MAGIC Spark arbeitet verteilt auf einem Cluster. Die wichtigsten Komponenten:
# MAGIC
# MAGIC - **Cluster Manager**:
# MAGIC   - Zuteilung von Ressourcen im Cluster (z. B. Standalone, YARN, Databricks-eigener Manager)
# MAGIC - **Driver**: 
# MAGIC   - Koordiniert die Berechnungen
# MAGIC   - Plant Tasks und verwaltet die SparkContext / SparkSession
# MAGIC - **Worker Nodes**:
# MAGIC   - Maschinen im Cluster, auf denen Executor-Prozesse laufen
# MAGIC - **Executors**:
# MAGIC   - Führen die Tasks auf den Worker Nodes aus
# MAGIC   - Jeder Executor verwaltet einen Teil des Arbeitsspeichers und der CPU-Kerne
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1.3. Lazy Evaluation
# MAGIC
# MAGIC - Spark führt **Transformationen** (z. B. `filter`, `select`) **nicht sofort** aus.
# MAGIC - Erst eine **Action** (z. B. `count()`, `show()`) löst die Berechnung aus.
# MAGIC - Vorteil: Optimierung durch Spark (z. B. Catalyst Optimizer, Pipeline-Fusion).
# MAGIC
# MAGIC Beispiel:  
# MAGIC
# MAGIC ```python
# MAGIC df_filtered = df.filter(df.Age > 30).select("Name")
# MAGIC # Keine Berechnung bisher
# MAGIC df_filtered.show()  # Jetzt werden die Daten berechnet
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2. PySpark im Überblick
# MAGIC
# MAGIC PySpark ist die **Python-Schnittstelle** zu Apache Spark.  
# MAGIC Damit können Python-Entwickler Spark-Funktionen nutzen, ohne Scala oder Java lernen zu müssen.
# MAGIC
# MAGIC **Kernideen von PySpark:**
# MAGIC - Zugriff auf Spark Core, Spark SQL, MLlib, GraphX und Streaming über Python
# MAGIC - Verteilte Datenverarbeitung auf Clustern
# MAGIC - Lazy Evaluation: Transformationen werden erst ausgeführt, wenn eine Action aufgerufen wird
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3. Komponenten von Apache Spark
# MAGIC
# MAGIC | Komponente | Beschreibung |
# MAGIC |------------|-------------|
# MAGIC | Spark Core | Kern von Spark, verantwortlich für Task Scheduling, Speicherverwaltung und Cluster-Management |
# MAGIC | Spark SQL  | Datenanalyse mit SQL-ähnlichen Abfragen, unterstützt DataFrames und Datasets |
# MAGIC | MLlib      | Machine Learning Bibliothek für verteilte ML-Algorithmen |
# MAGIC | Spark Streaming | Verarbeitung von Echtzeit-Datenströmen |
# MAGIC | GraphX     | Graph-Processing API |
# MAGIC
# MAGIC PySpark gibt uns Zugang zu all diesen Komponenten direkt aus Python heraus.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4. DataFrames und Transformationen
# MAGIC
# MAGIC In PySpark arbeiten wir meist mit **DataFrames**, die verteilt auf dem Cluster liegen.
# MAGIC
# MAGIC Wichtige Konzepte:
# MAGIC
# MAGIC - **Transformationen**: z. B. `select()`, `filter()`, `groupBy()` – werden **lazy** ausgeführt
# MAGIC - **Actions**: z. B. `show()`, `count()`, `collect()` – lösen die tatsächliche Berechnung aus
# MAGIC

# COMMAND ----------

# Beispiel

# 1. SparkSession erstellen

#INFO: Databricks erstellt eine spark Session automatisch, manuelle Erstellung in der Praxis nur notwendig fuer spezielle Settings wie hier am Beispiel gezeigt
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Einführung Apache Spark") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

# 2. Ein DataFrame erstellen
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Alter"]

df = spark.createDataFrame(data, columns)
df.show()

# 3. Transformation (Lazy)
df_filtered = df.filter(df.Alter > 30).select("Name")

# 4. Action
df_filtered.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5. Narrow vs. Wide Transformations
# MAGIC
# MAGIC Spark-Transformationen lassen sich in zwei Typen einteilen:
# MAGIC
# MAGIC #### **Narrow Transformations**
# MAGIC - Jede Partition des Input-RDDs/DFs wird nur auf **eine Partition des Outputs** gemappt.
# MAGIC - Kein Daten-Shuffling zwischen Nodes.
# MAGIC - Beispiele: `map()`, `filter()`, `select()`
# MAGIC - Vorteil: Sehr effizient, da keine Daten über das Netzwerk bewegt werden müssen.
# MAGIC
# MAGIC #### **Wide Transformations**
# MAGIC - Eine Partition des Outputs hängt von **mehreren Partitionen des Inputs** ab.
# MAGIC - Spark muss Daten zwischen Nodes verschieben → **Shuffle**.
# MAGIC - Beispiele: `groupByKey()`, `reduceByKey()`, `join()`
# MAGIC - Vorteil: notwendig für Aggregationen und komplexe Operationen, aber teurer in Performance.
# MAGIC
# MAGIC #### Shuffle in Spark
# MAGIC
# MAGIC **Shuffle** = das Umverteilen von Daten über Nodes im Cluster.  
# MAGIC - Entsteht bei **Wide Transformations**
# MAGIC - Spark schreibt Partitionen auf Disk oder Netzwerk, sendet sie an andere Executor-Nodes
# MAGIC - Teuer: Netzwerktransfer + Disk I/O + Sortierung
# MAGIC
# MAGIC **Warum ist Shuffle wichtig zu verstehen?**
# MAGIC - Vermeidet Überraschungen bei Performanceproblemen
# MAGIC - Hilft bei der Optimierung von Jobs

# COMMAND ----------

# Praxisbeispiel: Narrow vs. Wide Transformationen

data = [("Alice", "Math", 85),
        ("Bob", "Math", 90),
        ("Alice", "Physics", 95),
        ("Bob", "Physics", 80)]
columns = ["Name", "Fach", "Punkte"]

df = spark.createDataFrame(data, columns)

# Narrow Transformation (filter)
df_filtered = df.filter(df.Punkte > 85)
df_filtered.show()

# Wide Transformation (groupBy)
df_grouped = df.groupBy("Name").sum("Punkte")
df_grouped.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6. DataFrames vs. Spark SQL
# MAGIC
# MAGIC In Spark gibt es zwei Hauptmethoden, Daten zu verarbeiten: 
# MAGIC * **DataFrames** (API-basiert) und 
# MAGIC * **Spark SQL** (SQL-ähnliche Abfragen)
# MAGIC
# MAGIC Obwohl beide auf der gleichen Engine laufen und ähnliche Optimierungen nutzen, unterscheiden sie sich in Arbeitsweise, Syntax und Flexibilität.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6.1. DataFrames

# COMMAND ----------

#**API-basiert**: Python-, Scala-, oder R-Methoden  
#Unterstützt Transformationen und Aktionen direkt im Code  
#Vorteil: Integration mit komplexer Logik, Bedingungen, User-Defined Functions (UDFs)  

#Beispiel:
# DataFrame erstellen
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Alter"]

df = spark.createDataFrame(data, columns)

# Transformation: Filter + Select
df_filtered = df.filter(df.Alter > 30).select("Name")
df_filtered.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6.2. SQL
# MAGIC
# MAGIC - **SQL-basiert**: Daten mit bekannten SQL-Statements abfragen  
# MAGIC - Vorteil: Leicht verständlich für Analysten, Data Engineers und SQL-Profis  
# MAGIC - Transformationen werden ebenfalls **lazy** geplant und optimiert durch den **Catalyst Optimizer**  

# COMMAND ----------

# Beispiel:

# DataFrame als temporäre View registrieren
df.createOrReplaceTempView("personen")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from personen

# COMMAND ----------

new_df = spark.sql("select * from personen")
new_df.show()

new_df.write.format("delta").mode("overwrite").saveAsTable("personen_tabelle")

spark.sql("CREATE SCHEMA IF NOT EXISTS neues_schema")

new_df.write.format("delta").mode("overwrite").saveAsTable("neues_schema.personen_tabelle")

#spark.sql("DROP SCHEMA neues_schema CASCADE")

