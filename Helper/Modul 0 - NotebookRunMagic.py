# Databricks notebook source
# DataFrame erstellen
data = [
    (1, "Alice", 5000),
    (2, "Bob", 6000),
    (3, "Charlie", 7000)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)

# Registrierung als TempView
df.createOrReplaceTempView("temp_view")

# Funktion definieren
def durchschnitt_salary():
    return df.groupBy().avg("salary").collect()[0][0]

print("HilfsNotebookMagic wurde ausgeführt")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from temp_view
