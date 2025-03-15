import sys
sys.path.append('/home/nicholas/Documents/IOT_Weather')
import config as config
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, hour

# Initialize Spark session with MongoDB and PostgreSQL connectors
spark = SparkSession.builder \
    .appName("Bulk Import MongoDB to Postgres Weather Hour") \
    .config("spark.mongodb.input.uri", f"mongodb://{config.user}:{config.password}@{config.mongo_host}:{config.mongo_port}/{config.mongo_db}.{config.mongo_collection}?authSource=admin") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Load raw weather data from MongoDB
weather_df = spark.read.format("mongo") \
    .option("database", config.mongo_db) \
    .option("collection", config.mongo_collection) \
    .load()
weather_df.createOrReplaceTempView("weather_raw")

# Aggregate weather data by rounded coordinates, Date, and Hour
agg_sql = """
SELECT 
    ROUND(coord.lon, 2) AS lon, 
    ROUND(coord.lat, 2) AS lat,
    to_date(from_unixtime(dt)) AS Date,
    hour(from_unixtime(dt)) AS Hour,
    ROUND(AVG(main.temp), 2) AS Temp,
    ROUND(AVG(main.feels_like), 2) AS FeelsLike,
    ROUND(AVG(main.humidity), 2) AS Humidity,
    ROUND(AVG(COALESCE(rain.`1h`, 0)), 2) AS Rain,
    ROUND(AVG(wind.speed), 2) AS Wind,
    ROUND(AVG(clouds.all), 2) AS Clouds,
    ROUND(AVG(main.pressure), 2) AS Pressure
FROM weather_raw
GROUP BY ROUND(coord.lon, 2), ROUND(coord.lat, 2), to_date(from_unixtime(dt)), hour(from_unixtime(dt))
"""
agg_df = spark.sql(agg_sql)
agg_df.createOrReplaceTempView("agg_weather")

# Read city data from PostgreSQL using Spark JDBC
jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
postgres_properties = {
    "user": config.user,
    "password": config.password,
    "driver": "org.postgresql.Driver"
}
city_df = spark.read.jdbc(url=jdbc_url, table="city", properties=postgres_properties)
city_df.createOrReplaceTempView("cities")

# Join aggregated weather with city data to get city_id
join_sql = """
SELECT 
    c.idCity AS idCity,
    a.Date,
    a.Hour,
    a.Temp,
    a.FeelsLike,
    a.Clouds,
    a.Rain,
    a.Wind,
    a.Pressure,
    a.Humidity
FROM agg_weather a
JOIN cities c 
    ON a.lon = ROUND(c.lon,2)
   AND a.lat = ROUND(c.lat,2)
"""
final_df = spark.sql(join_sql)

# Write the final result to PostgreSQL weather_hour table
try:
    final_df.write.mode("append").jdbc(url=jdbc_url, table="weather_hour", properties=postgres_properties)
    print("Successfully bulk imported weather data to weather_hour table")
except Exception as e:
    print("Error during bulk import:", e)
finally:
    spark.stop()
