from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
sys.path.append('/home/nicholas/Documents/IOT_Weather')
import config as config

try:
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("MongoDB to PostGres Weather Condition Day") \
        .config("spark.mongodb.input.uri", f"mongodb://{config.user}:{config.password}@{config.mongo_host}:{config.mongo_port}/{config.mongo_db}.iot_weather?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # Calculate the target date (yesterday) and its start and end timestamps
    target_date = datetime.now().date() - timedelta(days=1)
    start_ts = datetime.combine(target_date, datetime.min.time()).timestamp()
    end_ts = datetime.combine(target_date + timedelta(days=1), datetime.min.time()).timestamp()

    # Load weather data from MongoDB and register as temporary view (use all records; filtering happens in SQL)
    weather_data = spark.read.format("mongo") \
        .option("database", config.mongo_db) \
        .option("collection", config.mongo_collection) \
        .load()
    weather_data.createOrReplaceTempView("weather_raw")

    # Use SparkSQL to filter records, explode the weather array, group and rank by count
    sql_query = f"""
        SELECT lon, lat, WeatherMain, WeatherDescription FROM (
            SELECT 
                ROUND(coord.lon, 2) AS lon,
                ROUND(coord.lat, 2) AS lat,
                weather_item.main AS WeatherMain,
                weather_item.description AS WeatherDescription,
                COUNT(*) AS cnt,
                ROW_NUMBER() OVER (PARTITION BY ROUND(coord.lon,2), ROUND(coord.lat,2) ORDER BY COUNT(*) DESC) AS rn
            FROM weather_raw
            LATERAL VIEW explode(weather) AS weather_item
            WHERE dt >= {start_ts} AND dt < {end_ts}
            GROUP BY ROUND(coord.lon, 2), ROUND(coord.lat, 2), weather_item.main, weather_item.description
        ) t
        WHERE rn = 1
    """
    temp_weather = spark.sql(sql_query)
    temp_weather.createOrReplaceTempView("temp_weather")

    # Read city data from PostgreSQL via Spark JDBC and register as temporary view
    jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
    postgres_properties = {
        "user": config.user,
        "password": config.password,
        "driver": "org.postgresql.Driver"
    }
    city_df = spark.read.jdbc(url=jdbc_url, table="city", properties=postgres_properties)
    city_df.createOrReplaceTempView("cities")

    # Join SparkSQL results with cities using SQL
    join_sql = """
        SELECT 
            c.IdCity AS city_id,
            t.WeatherMain,
            t.WeatherDescription
        FROM temp_weather t
        JOIN cities c
          ON ROUND(c.Lon, 2) = t.lon 
         AND ROUND(c.Lat, 2) = t.lat
    """
    result_df = spark.sql(join_sql)

    # Add target_date as the Date column
    result_df = result_df.withColumn("Date", lit(target_date).cast("date"))
    
    # Insert the results into the weather_condition_day table
    result_df.select(
        result_df.city_id.alias("IdCity"),
        result_df.Date,
        result_df.WeatherMain,
        result_df.WeatherDescription
    ).write.mode("append").jdbc(url=jdbc_url, table="weather_condition_day", properties=postgres_properties)

    print("Successfully inserted most common weather conditions into weather_condition_day table.")

except Exception as e:
    print("Error occurred:", e)
finally:
    spark.stop()