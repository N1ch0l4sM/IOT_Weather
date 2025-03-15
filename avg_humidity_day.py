from pyspark.sql import SparkSession
from datetime import date, timedelta
import config

# Compute yesterday's date
yesterday = date.today() - timedelta(days=1)

# Create Spark session
spark = SparkSession.builder \
    .appName("Aggregate Humidity Hour Data") \
    .getOrCreate()

# JDBC connection parameters using config
jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
connection_properties = {
    "user": config.user,
    "password": config.password,
    "driver": "org.postgresql.Driver"
}

# Read weather_hour table from PostgreSQL
humidity_df = spark.read.jdbc(url=jdbc_url, table="weather_hour", properties=connection_properties)
humidity_df.createOrReplaceTempView("weather_hour")

# Aggregate daily humidity values for yesterday (assuming the column 'Humidity' exists in weather_hour)
result_df = spark.sql(f"""
    SELECT 
        IdCity,
        Date,
        ROUND(MIN(Humidity), 2) AS HumidityMin,
        ROUND(MAX(Humidity), 2) AS HumidityMax,
        ROUND(AVG(Humidity), 2) AS HumidityAvg
    FROM weather_hour
    WHERE Date = '{yesterday}'
    GROUP BY IdCity, Date
""")

# Display aggregated results
result_df.show()

# Write the result into the humidity_day table
result_df.write.jdbc(url=jdbc_url, table="humidity_day", mode="append", properties=connection_properties)

spark.stop()