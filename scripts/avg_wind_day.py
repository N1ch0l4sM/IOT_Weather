from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys
sys.path.append('/home/nicholas/Documents/IOT_Weather')
import config as config

# Compute yesterday's date
yesterday = date.today() - timedelta(days=1)

# Create Spark session
spark = SparkSession.builder \
    .appName("Aggregate Wind Hour Data") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

# JDBC connection parameters using config
jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
connection_properties = {
    "user": config.user,
    "password": config.password,
    "driver": "org.postgresql.Driver"
}

# Read weather_hour table from PostgreSQL
weather_df = spark.read.jdbc(url=jdbc_url, table="weather_hour", properties=connection_properties)

# Register temporary view
weather_df.createOrReplaceTempView("weather_hour")

# Aggregate daily wind values for yesterday (assuming the column 'Wind' exists in weather_hour)
result_df = spark.sql(f"""
    SELECT 
        IdCity,
        Date,
        ROUND(MIN(Wind), 2) AS WindMin,
        ROUND(MAX(Wind), 2) AS WindMax,
        ROUND(AVG(Wind), 2) AS WindAvg
    FROM weather_hour
    WHERE Date = '{yesterday}'
    GROUP BY IdCity, Date
""")

# Display aggregated results
result_df.show()

# Write the results into the wind_day table
result_df.write.jdbc(url=jdbc_url, table="wind_day", mode="append", properties=connection_properties)

spark.stop()
