import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

# Configuration
CONFIG = {
    "MODEL_PATH_PRODUCTION": "/app/SparkML/models/production_model",
    "MODEL_PATH_CONSUMPTION": "/app/SparkML/models/consumption_model",
    "LAT_DK1": 56.2639, "LON_DK1": 9.5018,  # Central Jutland
    "LAT_DK2": 55.6761, "LON_DK2": 12.5683, # Copenhagen
}

def get_spark_session():
    return SparkSession.builder \
        .appName("EnergyForecaster") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

def fetch_weather_forecast(lat, lon, area_code):
    """
    Fetches 48-hour forecast from Open-Meteo API.
    Maps Open-Meteo fields to our model's feature names.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m,cloud_cover,wind_speed_10m,wind_direction_10m,direct_radiation,sunshine_duration",
        "forecast_days": 2
    }
    
    try:
        print(f"Fetching forecast for {area_code} from Open-Meteo...")
        response = requests.get(url, params=params)
        data = response.json()
        
        if 'hourly' not in data:
            print(f"Error: No hourly data in response: {data}")
            return pd.DataFrame()

        hourly = data['hourly']
        timestamps = hourly['time']
        
        records = []
        for i, ts_str in enumerate(timestamps):
            # Parse time
            dt = datetime.fromisoformat(ts_str)
            
            # Map features
            wind_speed = hourly['wind_speed_10m'][i] / 3.6 # km/h to m/s
            wind_dir = hourly['wind_direction_10m'][i]
            wind_rad = np.radians(wind_dir)
            
            record = {
                "timestamp": dt,
                "dk_area": area_code,
                
                # Temporal
                "month_of_year": dt.month,
                "hour_of_day": dt.hour,
                
                # Weather Features (Matching train_model.py)
                "wind_speed_avg": wind_speed,
                "wind_speed_max": wind_speed * 1.2, # Approximation
                "wind_direction_sin": float(np.sin(wind_rad)),
                "wind_direction_cos": float(np.cos(wind_rad)),
                "solar_radiation_1h": hourly['direct_radiation'][i],
                "sunshine_duration_1h": hourly['sunshine_duration'][i] / 60.0, # sec to min
                "cloud_cover_avg": hourly['cloud_cover'][i],
                
                # Constants (Model expects these)
                "n_stations_wind": 50, 
                "n_stations_solar": 50
            }
            records.append(record)
            
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"Error fetching forecast for {area_code}: {e}")
        return pd.DataFrame()

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Loading models...")
    try:
        prod_model = PipelineModel.load(CONFIG["MODEL_PATH_PRODUCTION"])
        cons_model = PipelineModel.load(CONFIG["MODEL_PATH_CONSUMPTION"])
        print("✓ Models loaded")
    except Exception as e:
        print(f"✗ Failed to load models: {e}")
        return

    # 1. Fetch Forecast Data
    df_dk1 = fetch_weather_forecast(CONFIG["LAT_DK1"], CONFIG["LON_DK1"], "DK1")
    df_dk2 = fetch_weather_forecast(CONFIG["LAT_DK2"], CONFIG["LON_DK2"], "DK2")
    
    full_pdf = pd.concat([df_dk1, df_dk2])
    
    if full_pdf.empty:
        print("No weather data fetched.")
        return

    # 2. Convert to Spark DataFrame
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("dk_area", StringType(), True),
        StructField("month_of_year", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("wind_speed_avg", DoubleType(), True),
        StructField("wind_speed_max", DoubleType(), True),
        StructField("wind_direction_sin", DoubleType(), True),
        StructField("wind_direction_cos", DoubleType(), True),
        StructField("solar_radiation_1h", DoubleType(), True),
        StructField("sunshine_duration_1h", DoubleType(), True),
        StructField("cloud_cover_avg", DoubleType(), True),
        StructField("n_stations_wind", IntegerType(), True),
        StructField("n_stations_solar", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(full_pdf, schema=schema)
    
    # 3. Run Predictions
    print("Running predictions...")
    
    # Predict Production
    pred_prod = prod_model.transform(df)
    pred_prod = pred_prod.withColumnRenamed("prediction", "predicted_production")
    
    # Predict Consumption
    pred_cons = cons_model.transform(df)
    pred_cons = pred_cons.withColumnRenamed("prediction", "predicted_consumption")
    
    # Join results
    results = pred_prod.select("timestamp", "dk_area", "predicted_production") \
        .join(pred_cons.select("timestamp", "dk_area", "predicted_consumption"), 
              on=["timestamp", "dk_area"]) \
        .withColumn("net_balance", F.col("predicted_production") - F.col("predicted_consumption")) \
        .orderBy("timestamp", "dk_area")
    
    # 4. Output Results
    print("\n--- FORECAST RESULTS (Next 48 Hours) ---")
    results.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()