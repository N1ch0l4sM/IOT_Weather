from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys
sys.path.append('/home/nicholas/Documents/IOT_Weather')
import config as config

# Compute yesterday's date
yesterday = date.today() - timedelta(days=1)

# ...existing code to create Spark session...
spark = SparkSession.builder \
    .appName("Aggregate Rain Hour Data") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

# JDBC connection parameters using config
jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
connection_properties = {
    "user": config.user,
    "password": config.password,
    "driver": "org.postgresql.Driver"
}

# ...existing code to read weather_hour table...
rain_df = spark.read.jdbc(url=jdbc_url, table="weather_hour", properties=connection_properties)
rain_df.createOrReplaceTempView("weather_hour")

# Aggregate daily rain values for yesterday (assuming the column 'Rain' exists in weather_hour)
result_df = spark.sql(f"""
    SELECT 
        IdCity,
        Date,
        ROUND(MIN(Rain), 2) AS RainMin,
        ROUND(MAX(Rain), 2) AS RainMax,
        ROUND(AVG(Rain), 2) AS RainAvg
    FROM weather_hour
    WHERE Date = '{yesterday}'
    GROUP BY IdCity, Date
""")

# ...existing code to show and write the results...
result_df.show()

result_df.write.jdbc(url=jdbc_url, table="rain_day", mode="append", properties=connection_properties)

spark.stop()
