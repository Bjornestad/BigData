import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import sys
import os
from unittest.mock import MagicMock, patch

# Add SparkML directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../Docker/prediction-service/SparkML'))

# Mock environment
os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'localhost:9092'

# Import module (will fail if SparkSession creation isn't mocked or handled)
# We'll use a patch to prevent the module from auto-initializing Spark/Kafka on import if possible,
# or just let it initialize and mock the internals.
# Since the script runs main() only on __name__ == "__main__", import is safe.
import aggregate_and_predict_service

@pytest.fixture(scope="session")
def spark():
    """Create local SparkSession"""
    spark = SparkSession.builder \
        .appName("TestAggregateService") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_raw_weather_df(spark):
    """Create mock raw weather data (long format)"""
    schema = StructType([
        StructField("stationId", StringType(), True),
        StructField("parameterId", StringType(), True),
        StructField("timeObserved", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Create data for a specific 10-min bucket (e.g., 10:00-10:10)
    # Station 06180 is in DK2 (based on script logic)
    # Station 05005 is in DK1
    data = [
        # DK2 Station - Temp
        ("06180", "temp_dry", "2023-10-27T10:05:00Z", 15.0),
        ("06180", "temp_dry", "2023-10-27T10:08:00Z", 16.0),
        # DK2 Station - Wind
        ("06180", "wind_speed", "2023-10-27T10:05:00Z", 5.0),
        
        # DK1 Station - Temp
        ("05005", "temp_dry", "2023-10-27T10:02:00Z", 10.0),
        ("05005", "temp_dry", "2023-10-27T10:09:00Z", 12.0)
    ]
    
    return spark.createDataFrame(data, schema)

def test_station_to_dk_area():
    """Test station mapping logic"""
    assert aggregate_and_predict_service.station_to_dk_area("05005") == "DK1"
    assert aggregate_and_predict_service.station_to_dk_area("06180") == "DK2"
    assert aggregate_and_predict_service.station_to_dk_area("99999") == "UNKNOWN"

def test_aggregation_logic(spark, mock_raw_weather_df):
    """
    Test the core aggregation logic by manually running the transformation steps
    extracted from aggregate_latest_10min
    """
    # 1. Filter for specific bucket (10:00 - 10:10)
    bucket_start = "2023-10-27T10:00:00Z"
    bucket_end = "2023-10-27T10:10:00Z"
    
    from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, minute, lit
    import pyspark.sql.functions as F
    
    # Apply transformations from the script
    bucket_df = mock_raw_weather_df.filter(
        (col("timeObserved") >= bucket_start) & 
        (col("timeObserved") < bucket_end)
    ) \
    .withColumn("observed_ts", to_timestamp(col("timeObserved"))) \
    .withColumn("year", year(col("observed_ts"))) \
    .withColumn("month", month(col("observed_ts"))) \
    .withColumn("day", dayofmonth(col("observed_ts"))) \
    .withColumn("hour", hour(col("observed_ts"))) \
    .withColumn("minute", minute(col("observed_ts"))) \
    .withColumn("minute_bucket", (col("minute") / 10).cast("int") * 10)
    
    # Pivot
    wide_df = bucket_df.groupBy("stationId", "year", "month", "day", "hour", "minute_bucket") \
        .pivot("parameterId") \
        .avg("value")
        
    # Add DK Area
    # We need to register the UDF or use the python function directly if testing locally
    # Since UDF registration depends on the global spark session in the script, 
    # we'll use a map/udf here for the test
    from pyspark.sql.functions import udf
    station_mapper = udf(aggregate_and_predict_service.station_to_dk_area, StringType())
    
    wide_df = wide_df.withColumn("dk_area", station_mapper(col("stationId")))
    
    # Aggregate by Area
    # Simplified aggregation for test (just temp and wind)
    agg_df = wide_df.groupBy("dk_area").agg(
        F.avg("temp_dry").alias("temp_mean_area"),
        F.avg("wind_speed").alias("wind_speed_mean_area")
    )
    
    # Collect results
    results = agg_df.collect()
    results_dict = {row['dk_area']: row for row in results}
    
    # Verify DK2 (Station 06180)
    # Temp: (15.0 + 16.0) / 2 = 15.5
    # Wind: 5.0
    assert "DK2" in results_dict
    assert results_dict["DK2"]["temp_mean_area"] == 15.5
    assert results_dict["DK2"]["wind_speed_mean_area"] == 5.0
    
    # Verify DK1 (Station 05005)
    # Temp: (10.0 + 12.0) / 2 = 11.0
    # Wind: None (null)
    assert "DK1" in results_dict
    assert results_dict["DK1"]["temp_mean_area"] == 11.0
    # Spark avg of nulls is null
    assert results_dict["DK1"]["wind_speed_mean_area"] is None

@patch('aggregate_and_predict_service.consumption_model')
@patch('aggregate_and_predict_service.producer')
def test_make_predictions_logic(mock_producer, mock_model, spark):
    """Test prediction logic with mocked model and producer"""
    # Mock Spark session in the module
    aggregate_and_predict_service.spark = spark
    
    # Create mock weather data ready for prediction
    schema = StructType([
        StructField("dk_area", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute_bucket", IntegerType(), True),
        StructField("temp_mean_area", DoubleType(), True),
        StructField("wind_speed_mean_area", DoubleType(), True),
        StructField("n_stations", IntegerType(), True),
        # Add other required columns
        StructField("temp_max_area", DoubleType(), True),
        StructField("temp_min_area", DoubleType(), True),
        StructField("wind_speed_max_area", DoubleType(), True),
        StructField("wind_dir_sin_area", DoubleType(), True),
        StructField("wind_dir_cos_area", DoubleType(), True),
        StructField("sun_last10min_glob_area", DoubleType(), True),
        StructField("precip_past10min_mean_area", DoubleType(), True),
        StructField("humidity_mean_area", DoubleType(), True),
        StructField("pressure_at_sea_mean_area", DoubleType(), True),
        StructField("cloud_cover_mean_area", DoubleType(), True),
        StructField("visibility_mean_area", DoubleType(), True)
    ])
    
    # Create a DataFrame that mimics 'weather_area_10min'
    weather_df = spark.createDataFrame([
        ("DK1", 2023, 10, 27, 10, 0, 12.0, 5.0, 5, 13.0, 11.0, 8.0, 0.5, 0.5, 0.0, 0.0, 80.0, 1013.0, 50.0, 10000.0)
    ], schema)
    
    # Mock the SQL queries
    # 1. Query for unpredicted data
    with patch.object(spark, 'sql', return_value=weather_df):
        # Mock model transformation
        # The model should return the input DF with a 'prediction' column
        mock_predictions = weather_df.withColumn("prediction", F.lit(123.45))
        mock_model.transform.return_value = mock_predictions
        
        # Run the function
        aggregate_and_predict_service.make_predictions()
        
        # Verify producer was called
        assert mock_producer.send.called
        args, kwargs = mock_producer.send.call_args
        
        # Check topic
        assert args[0] == aggregate_and_predict_service.OUTPUT_TOPIC
        
        # Check payload
        payload = kwargs['value']
        assert payload['dk_area'] == "DK1"
        assert payload['value'] == 123.45
        assert payload['predictions']['consumption_mwh'] == 123.45
