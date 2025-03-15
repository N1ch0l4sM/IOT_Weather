import argparse
from datetime import datetime
from pyspark.sql import SparkSession
import config

def parse_args():
    parser = argparse.ArgumentParser(description="Aggregate weather data over a custom date range")
    parser.add_argument('--start-date', type=str, required=True, help="Start date in YYYY-MM-DD format")
    parser.add_argument('--end-date', type=str, required=True, help="End date in YYYY-MM-DD format")
    return parser.parse_args()

def main():
    args = parse_args()
    start_date = args.start_date
    end_date = args.end_date

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Bulk Aggregate Weather Data") \
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

    # Aggregate Pressure
    pressure_query = f"""
        SELECT 
            IdCity, 
            Date, 
            ROUND(MIN(Pressure), 2) AS PressureMin,
            ROUND(MAX(Pressure), 2) AS PressureMax,
            ROUND(AVG(Pressure), 2) AS PressureAvg
        FROM weather_hour
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY IdCity, Date
    """
    pressure_df = spark.sql(pressure_query)
    pressure_df.show()
    pressure_df.write.jdbc(url=jdbc_url, table="pressure_day", mode="append", properties=connection_properties)

    # Aggregate Humidity
    humidity_query = f"""
        SELECT 
            IdCity, 
            Date, 
            ROUND(MIN(Humidity), 2) AS HumidityMin,
            ROUND(MAX(Humidity), 2) AS HumidityMax,
            ROUND(AVG(Humidity), 2) AS HumidityAvg
        FROM weather_hour
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY IdCity, Date
    """
    humidity_df = spark.sql(humidity_query)
    humidity_df.show()
    humidity_df.write.jdbc(url=jdbc_url, table="humidity_day", mode="append", properties=connection_properties)

    # Aggregate Temperature
    temp_query = f"""
        SELECT 
            IdCity, 
            Date, 
            ROUND(MIN(Temp), 2) AS TempMin,
            ROUND(MAX(Temp), 2) AS TempMax,
            ROUND(AVG(Temp), 2) AS TempAvg,
            ROUND(AVG(FeelsLike), 2) AS AvgFeelsLike
        FROM weather_hour
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY IdCity, Date
    """
    temp_df = spark.sql(temp_query)
    temp_df.show()
    temp_df.write.jdbc(url=jdbc_url, table="temperature_day", mode="append", properties=connection_properties)

    # Aggregate Wind
    wind_query = f"""
        SELECT 
            IdCity, 
            Date, 
            ROUND(MIN(Wind), 2) AS WindMin,
            ROUND(MAX(Wind), 2) AS WindMax,
            ROUND(AVG(Wind), 2) AS WindAvg
        FROM weather_hour
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY IdCity, Date
    """
    wind_df = spark.sql(wind_query)
    wind_df.show()
    wind_df.write.jdbc(url=jdbc_url, table="wind_day", mode="append", properties=connection_properties)

    # Aggregate Rain
    rain_query = f"""
        SELECT 
            IdCity, 
            Date, 
            ROUND(MIN(Rain), 2) AS RainMin,
            ROUND(MAX(Rain), 2) AS RainMax,
            ROUND(AVG(Rain), 2) AS RainAvg
        FROM weather_hour
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY IdCity, Date
    """
    rain_df = spark.sql(rain_query)
    rain_df.show()
    rain_df.write.jdbc(url=jdbc_url, table="rain_day", mode="append", properties=connection_properties)

    spark.stop()

if __name__ == "__main__":
    main()
