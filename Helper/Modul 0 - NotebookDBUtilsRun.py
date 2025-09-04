# Databricks notebook source
# Eingabeparameter als Widget
dbutils.widgets.text("min_salary", "5000")
min_salary = int(dbutils.widgets.get("min_salary"))

# DataFrame erstellen
data = [
    (1, "Alice", 5000),
    (2, "Bob", 6000),
    (3, "Charlie", 7000)
]
columns = ["id", "name", "salary"]
df = spark.createDataFrame(data, columns)

# Filter anwenden
df_filtered = df.filter(df.salary >= min_salary)

# Rückgabe an aufrufendes Notebook (nur Strings möglich)
dbutils.notebook.exit(str(df_filtered.collect()))

