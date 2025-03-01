from pymongo import MongoClient
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
import config
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MongoDB to PostGres") \
    .config("spark.mongodb.input.uri", f"mongodb://{config.user}:{config.password}@{config.mongo_host}:{config.mongo_port}/{config.mongo_db}.iot_weather") \
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
client = MongoClient(mongo_host, mongo_port, username=config.user, password=config.password)
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
    ROUND(AVG(wind.speed), 2) AS avg_wind
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
    w.lon,
    w.lat
FROM avg_weather w
JOIN cities c 
    ON ROUND(w.lat, 2) = ROUND(c.lat, 2)
    AND ROUND(w.lon, 2) = ROUND(c.lon, 2)
"""

result_df = spark.sql(joined_sql)

# Show the joined results
result_df.show()

# Get current date and hour
current_date = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d")
one_hour_ago_num = datetime.now().hour

# Convert DataFrame to a list of tuples for batch insertion
rows_to_insert = [(
    row.city_id, 
    row.avg_temperature, 
    row.avg_humidity, 
    row.avg_rain, 
    row.avg_wind, 
    current_date, 
    one_hour_ago_num
) for row in result_df.collect()]

# Connect to PostgreSQL for insertion
postgre_conn = psycopg2.connect(
    dbname=config.postG_db,
    user=config.user,
    password=config.password,
    host=config.postG_host,
    port=config.postG_port
)
cursor = postgre_conn.cursor()

# Insert data into PostgreSQL
if rows_to_insert:
    cursor.executemany("""
    INSERT INTO avg_hour ("idCity", "AvgTemp", "AvgHumidity", "AvgRain", "AvgWind", "Date", "Hour")
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows_to_insert)
    postgre_conn.commit()
    print(f"Successfully inserted {len(rows_to_insert)} records into avg_hour table")
else:
    print("No new data to insert.")

# Close the PostgreSQL connection
cursor.close()
postgre_conn.close()

# Close the MongoDB connection
client.close()

# Stop the Spark session
spark.stop()