data =  {
        "Sr": 1,
        "Country": "USA",
        "City": "New York",
        "Date": "2023-05-11",
        "Temperature": 20,
        "Humidity": 60,
        "Wind Speed": 10,
        "Wind Direction": "NW",
        "Precipitation": 0,
        "Cloud Cover": 20,
        "Visibility": 10,
        "Pressure": 1013,
        "Dew Point": 10,
        "UV Index": 5,
        "Sunrise": "05:30",
        "Sunset": "20:15",
        "Moonrise": "22:00",
        "Moonset": "08:00",
        "Moon Phase": "Waning Gibbous",
        "Conditions": "Partly Cloudy",
        "Icon": "partly-cloudy-day"
    }

from pyspark.sql import SparkSession

# Set up a Spark session
spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.9.1") \
    .getOrCreate()

# Replace these placeholders with your Azure Storage account and container names
azure_storage_account = "ssahildemo123"
container_name = "test"

# Configure Azure Storage settings
spark.conf.set(
    "fs.azure.account.key.{0}.dfs.core.windows.net".format(azure_storage_account),
    "8wu4C81sRlIh/ml24S/sKOmNS2B0QlwQaf29A9NF5GrLWVVbqxK5IjxOedWS4cEMEOqRptQj4lcF+ASt1MKpMg=="
)

# Create a PySpark DataFrame
df = spark.read.json(data)

# Define the path where you want to store the Delta table in Azure Storage
delta_path = "abfss://test@ssahildemo123.dfs.core.windows.net/delta-table"

# Write the DataFrame to Delta format
df.write.format("delta").mode("overwrite").save(delta_path)

# Optionally, you can optimize the Delta table
spark.sql(f"OPTIMIZE delta.`{delta_path}`")

# Stop the Spark session
spark.stop()









