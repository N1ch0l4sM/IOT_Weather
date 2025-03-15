from datetime import date, timedelta
from pyspark.sql import SparkSession
import config

try:
    spark = SparkSession.builder \
        .appName("Monthly Avg Calculation for Weather") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    jdbc_url = f"jdbc:postgresql://{config.postG_host}:{config.postG_port}/{config.postG_db}"
    jdbc_props = {
        "user": config.user,
        "password": config.password,
        "driver": "org.postgresql.Driver"
    }

    # Load daily weather tables from Postgres
    temp_df = spark.read.jdbc(url=jdbc_url, table="temperature_day", properties=jdbc_props)
    hum_df = spark.read.jdbc(url=jdbc_url, table="humidity_day", properties=jdbc_props)
    rain_df = spark.read.jdbc(url=jdbc_url, table="rain_day", properties=jdbc_props)
    wind_df = spark.read.jdbc(url=jdbc_url, table="wind_day", properties=jdbc_props)

    # Create temporary views for SparkSQL queries
    temp_df.createOrReplaceTempView("temperature_day")
    hum_df.createOrReplaceTempView("humidity_day")
    rain_df.createOrReplaceTempView("rain_day")
    wind_df.createOrReplaceTempView("wind_day")

  # Compute yesterday's date
    month = (date.today() - timedelta(days=1)).month
    year = (date.today() - timedelta(days=1)).year

    # Use SparkSQL with CTEs to aggregate monthly data per city
    agg_query = """
        WITH temp_agg AS (
            SELECT 
                IdCity, 
                EXTRACT(YEAR FROM Date) AS Year, 
                EXTRACT(MONTH FROM Date) AS Month,
                AVG(TempAvg) AS AvgTemp,
                MIN(TempMin) AS MinTemp,
                MAX(TempMax) AS MaxTemp
            FROM temperature_day
            WHERE EXTRACT(MONTH FROM Date) = {month} and EXTRACT(YEAR FROM Date) = {year}
            GROUP BY IdCity, EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date)
        ),
        hum_agg AS (
            SELECT 
                IdCity, 
                EXTRACT(YEAR FROM Date) AS Year, 
                EXTRACT(MONTH FROM Date) AS Month,
                AVG(HumidityAvg) AS AvgHumidity
            FROM humidity_day
            WHERE EXTRACT(MONTH FROM Date) = {month} and EXTRACT(YEAR FROM Date) = {year}
            GROUP BY IdCity, EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date)
        ),
        rain_agg AS (
            SELECT 
                IdCity, 
                EXTRACT(YEAR FROM Date) AS Year, 
                EXTRACT(MONTH FROM Date) AS Month,
                AVG(RainAvg) AS AvgRain
            FROM rain_day
            WHERE EXTRACT(MONTH FROM Date) = {month} and EXTRACT(YEAR FROM Date) = {year}
            GROUP BY IdCity, EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date)
        ),
        wind_agg AS (
            SELECT 
                IdCity, 
                EXTRACT(YEAR FROM Date) AS Year, 
                EXTRACT(MONTH FROM Date) AS Month,
                AVG(WindAvg) AS AvgWind
            FROM wind_day
            WHERE EXTRACT(MONTH FROM Date) = {month} and EXTRACT(YEAR FROM Date) = {year}
            GROUP BY IdCity, EXTRACT(YEAR FROM Date), EXTRACT(MONTH FROM Date)
        )
        SELECT 
            t.IdCity, 
            t.Year, 
            t.Month, 
            t.AvgTemp, 
            (t.MaxTemp - t.MinTemp) AS TempVariation,
            h.AvgHumidity, 
            r.AvgRain, 
            w.AvgWind
        FROM temp_agg t
        JOIN hum_agg h ON t.IdCity = h.IdCity AND t.Year = h.Year AND t.Month = h.Month
        JOIN rain_agg r ON t.IdCity = r.IdCity AND t.Year = r.Year AND t.Month = r.Month
        JOIN wind_agg w ON t.IdCity = w.IdCity AND t.Year = w.Year AND t.Month = w.Month
    """
    final_df = spark.sql(agg_query)

    # Write the results to the AVG_month table in Postgres
    final_df.write.jdbc(url=jdbc_url, table="AVG_month", mode="append", properties=jdbc_props)

    print("Successfully calculated and inserted monthly averages into AVG_month table.")

except Exception as e:
    print("Error occurred:", e)
finally:
    spark.stop()