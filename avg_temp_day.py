from pyspark.sql import SparkSession
import config  # added to use config parameters
from datetime import date, timedelta  # added to compute yesterday's date

# Compute yesterday's date
yesterday = date.today() - timedelta(days=1)

# Create Spark session
spark = SparkSession.builder \
    .appName("Aggregate Weather Hour Data") \
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

# Aggregate daily temperature values and average feels like only for yesterday
result_df = spark.sql(f"""
    SELECT 
        IdCity,
        Date,
        ROUND(MIN(Temp), 2) AS TempMin,
        ROUND(MAX(Temp), 2) AS TempMax,
        ROUND(AVG(Temp), 2) AS TempAvg,
        ROUND(AVG(FeelsLike), 2) AS AvgFeelsLike
    FROM weather_hour
    WHERE Date = '{yesterday}'
    GROUP BY IdCity, Date
""")

# Display the aggregated results
result_df.show()

# Optionally, write back to the temperature_day table
result_df.write.jdbc(url=jdbc_url, table="temperature_day", mode="append", properties=connection_properties)

spark.stop()