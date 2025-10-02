dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")

CATALOG = dbutils.widgets.get("catalog")
print("CATALOG:")
print(CATALOG)
SCHEMA = dbutils.widgets.get("schema")
print("SCHEMA:")
print(SCHEMA)

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

# Row Count
df_taxi.count()

# Statistik
#df_taxi.describe().show()
df_taxi.describe("trip_distance").show()

# Filter 
long_trips = df_taxi.filter(df_taxi.trip_distance > 10)
short_long_trips = df_taxi.filter((df_taxi.trip_distance > 10) | (df_taxi.trip_distance < 1))

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
