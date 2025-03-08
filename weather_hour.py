from pymongo import MongoClient
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, lit
import config


# Initialize Spark session
spark = SparkSession.builder \
    .appName("MongoDB to PostGres") \
    .config("spark.mongodb.input.uri", f"mongodb://{config.user}:{config.password}@{config.mongo_host}:{config.mongo_port}/{config.mongo_db}.iot_weather?authSource=admin") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# MongoDB connection setup
mongo_host = config.mongo_host
mongo_port = config.mongo_port
mongo_db = config.mongo_db
mongo_collection = "iot_weather"

# PostgreSQL connection properties
postgres_properties = {
    "user": config.user,
    "password": config.password,
    "driver": "org.postgresql.Driver"
}

# Connect to MongoDB
client = MongoClient(mongo_host, mongo_port, username=config.user, password=config.password, authSource="admin")
db = client[mongo_db]
collection = db[mongo_collection]

# Calculate the date one hour ago from today
one_hour_ago_timestamp = (datetime.now() - timedelta(hours=1)).timestamp()

# Load data from MongoDB, filtering for data from one hour ago
weather_data = spark.read.format("mongo") \
    .option("database", mongo_db) \
    .option("collection", mongo_collection) \
    .load() \
    .filter(col("dt") >= one_hour_ago_timestamp)

# Register the data frame as a temporary view for SQL queries
weather_data.createOrReplaceTempView("weather_raw")

# SQL query to calculate averages with proper handling of null rain values
avg_temps_sql = """
SELECT 
    ROUND(coord.lon, 2) AS lon, 
    ROUND(coord.lat, 2) AS lat,
    ROUND(AVG(main.temp), 2) AS avg_temperature,
    ROUND(AVG(main.humidity), 2) AS avg_humidity,
    ROUND(AVG(COALESCE(rain.`1h`, 0)), 2) AS avg_rain,
    ROUND(AVG(wind.speed), 2) AS avg_wind,
    ROUND(AVG(clouds.all), 2) AS avg_clouds,
    ROUND(AVG(main.feels_like), 2) AS avg_feels_like
FROM weather_raw
GROUP BY ROUND(coord.lon, 2), ROUND(coord.lat, 2)
"""

avg_temps = spark.sql(avg_temps_sql)
avg_temps.createOrReplaceTempView("avg_weather")

# Read city data directly from PostgreSQL using Spark JDBC
city_df = spark.read \
    .jdbc(
        url=f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}",
        table="city",
        properties=postgres_properties
    )
city_df.createOrReplaceTempView("cities")

# SQL join query to match city data with weather averages
joined_sql = """
SELECT 
    c.idCity AS city_id,
    c.CityName AS city_name,
    w.avg_temperature,
    w.avg_humidity,
    w.avg_rain,
    w.avg_wind,
    w.avg_clouds,
    w.avg_feels_like,
    w.lon,
    w.lat
FROM avg_weather w
JOIN cities c 
    ON ROUND(w.lat, 2) = ROUND(c.lat, 2)
    AND ROUND(w.lon, 2) = ROUND(c.lon, 2)
"""

result_df = spark.sql(joined_sql)

try:
    # Show the joined results
    result_df.show()
    
    # Compute one-hour–ago time for consistency
    one_hour_ago = datetime.now() - timedelta(hours=1)
    current_date = one_hour_ago.strftime("%Y-%m-%d")
    one_hour_ago_num = one_hour_ago.hour
    
    # Cast current_date to date type
    result_df = result_df.withColumn("Date", lit(current_date).cast("date")) \
                         .withColumn("Hour", lit(one_hour_ago_num))
    
    jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
    
    # Modified insert mapping for weather_hour table
    result_df.select(
        col("city_id").alias("idCity"),
        col("Date"),
        col("Hour"),
        col("avg_temperature").alias("Temp"),
        col("avg_feels_like").alias("FeelsLike"),  
        col("avg_clouds").alias("Clouds"),           
        col("avg_rain").alias("Rain")
    ).write.mode("append").jdbc(url=jdbc_url, table="weather_hour", properties=postgres_properties)
    
    print("Successfully appended records to weather_hour table using PySpark")
except Exception as e:
    print("Error occurred:", e)
finally:
    # Close the MongoDB connection
    client.close()
    # Stop the Spark session
    spark.stop()