from pyspark.sql import SparkSession
import datetime
import sys
sys.path.append('/home/nicholas/Documents/IOT_Weather')
import config

try:
    # Initialize Spark session with MongoDB and PostgreSQL packages
    spark = SparkSession.builder \
        .appName("Bulk Insert Weather Condition Day") \
        .config("spark.mongodb.input.uri", f"mongodb://{config.user}:{config.password}@{config.mongo_host}:{config.mongo_port}/{config.mongo_db}.{config.mongo_collection}?authSource=admin") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # Compute cutoff timestamp (today's midnight) so that all data until yesterday are included
    today = datetime.date.today()
    cutoff_dt = datetime.datetime.combine(today, datetime.time.min)
    cutoff_ts = cutoff_dt.timestamp()

    # Load weather data from MongoDB and register as temporary view
    df = spark.read.format("mongo") \
         .option("database", config.mongo_db) \
         .option("collection", config.mongo_collection) \
         .load()
    df.createOrReplaceTempView("raw_weather")
    
    sql_query = f"""
        SELECT lon, lat, Date, WeatherMain, WeatherDescription FROM (
            SELECT 
                ROUND(coord.lon, 2) AS lon,
                ROUND(coord.lat, 2) AS lat,
                from_unixtime(dt, 'yyyy-MM-dd') AS Date,
                weather_item.main AS WeatherMain,
                weather_item.description AS WeatherDescription,
                COUNT(*) AS cnt,
                ROW_NUMBER() OVER (
                    PARTITION BY ROUND(coord.lon, 2), ROUND(coord.lat, 2), from_unixtime(dt, 'yyyy-MM-dd')
                    ORDER BY COUNT(*) DESC
                ) AS rn
            FROM raw_weather
            LATERAL VIEW explode(weather) AS weather_item
            WHERE dt < {cutoff_ts}
            GROUP BY ROUND(coord.lon, 2), ROUND(coord.lat, 2), from_unixtime(dt, 'yyyy-MM-dd'), weather_item.main, weather_item.description
        ) t
        WHERE rn = 1
    """
    temp_weather = spark.sql(sql_query)
    temp_weather.createOrReplaceTempView("temp_weather")
    
    # Read cities table from PostgreSQL as temporary view
    jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
    postgres_properties = {
        "user": config.user,
        "password": config.password,
        "driver": "org.postgresql.Driver"
    }
    city_df = spark.read.jdbc(url=jdbc_url, table="city", properties=postgres_properties)
    city_df.createOrReplaceTempView("cities")
    
    # Change join_sql to cast the Date column to date type
    join_sql = """
        SELECT 
            c.IdCity AS IdCity,
            CAST(t.Date AS DATE) AS Date,
            t.WeatherMain,
            t.WeatherDescription
        FROM temp_weather t
        JOIN cities c
          ON ROUND(c.Lon, 2) = t.lon 
         AND ROUND(c.Lat, 2) = t.lat
    """
    result_df = spark.sql(join_sql)
    
    result_df.write.mode("append").jdbc(url=jdbc_url, table="weather_condition_day", properties=postgres_properties)
    
    print("Successfully inserted bulk weather condition data into weather_condition_day table.")

except Exception as e:
    print("Error occurred:", e)
finally:
    spark.stop()
