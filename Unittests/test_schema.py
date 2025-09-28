import pytest

CATALOG = "workspace"
SCHEMA = "default"
DATA_PATH = f"{CATALOG}.{SCHEMA}.yellow_tripdata_2025_01"
LOOKUP_PATH = f"{CATALOG}.{SCHEMA}.taxi_zone_lookup"

def test_tripdata_schema(spark):
    df_taxi = spark.read.table(DATA_PATH)
    expected_fields = {
        "VendorID": "string",
        "tpep_pickup_datetime": "timestamp",
        "tpep_dropoff_datetime": "timestamp",
        "passenger_count": "double",
        "trip_distance": "double",
        "RatecodeID": "double",
        "store_and_fwd_flag": "string",
        "PULocationID": "int",
        "DOLocationID": "int",
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
    for field in df_taxi.schema.fields:
        assert field.name in expected_fields, f"{field.name} not in expected fields"
        assert field.dataType.simpleString() == expected_fields[field.name], f"Type mismatch for {field.name}"

def test_zone_lookup_schema(spark):
    df_lookup = spark.read.table(LOOKUP_PATH)
    expected_fields = {
        "LocationID": "int",
        "Borough": "string",
        "Zone": "string",
        "service_zone": "string"
    }
    for field in df_lookup.schema.fields:
        assert field.name in expected_fields, f"{field.name} not in expected fields"
        assert field.dataType.simpleString() == expected_fields[field.name], f"Type mismatch for {field.name}"

if __name__ == "__main__":
    test_tripdata_schema(spark)
    test_zone_lookup_schema(spark)
    print("All schema tests passed!")