import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys
import os

# Add the SparkML directory to the path so we can import the module
sys.path.append(os.path.join(os.path.dirname(__file__), '../Docker/prediction-service/SparkML'))

# Mock the environment variables
os.environ['HIVE_METASTORE_URI'] = 'thrift://localhost:9083'

# Import the module to be tested
import train_consumption_model

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing"""
    spark = SparkSession.builder \
        .appName("TestTrainConsumptionModel") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_weather_data(spark):
    """Create mock weather data"""
    schema = StructType([
        StructField("dk_area", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("temp_mean_area", DoubleType(), True),
        StructField("wind_speed_mean_area", DoubleType(), True),
        StructField("n_stations", IntegerType(), True),
        # Add other required columns with nulls or defaults
        StructField("temp_max_area", DoubleType(), True),
        StructField("temp_min_area", DoubleType(), True),
        StructField("temp_grass_mean_area", DoubleType(), True),
        StructField("temp_soil_mean_area", DoubleType(), True),
        StructField("wind_speed_max_area", DoubleType(), True),
        StructField("wind_dir_sin_area", DoubleType(), True),
        StructField("wind_dir_cos_area", DoubleType(), True),
        StructField("wind_gust_always_past1h_max_area", DoubleType(), True),
        StructField("radia_glob_past1h_area", DoubleType(), True),
        StructField("sun_last1h_glob_area", DoubleType(), True),
        StructField("sun_last10min_glob_area", DoubleType(), True),
        StructField("precip_past1h_mean_area", DoubleType(), True),
        StructField("precip_past10min_mean_area", DoubleType(), True),
        StructField("humidity_mean_area", DoubleType(), True),
        StructField("pressure_at_sea_mean_area", DoubleType(), True),
        StructField("cloud_cover_mean_area", DoubleType(), True),
        StructField("visibility_mean_area", DoubleType(), True)
    ])
    
    data = [
        ("DK1", 2023, 1, 1, 12, 5.0, 10.0, 5, 6.0, 4.0, 0.0, 0.0, 12.0, 0.5, 0.5, 15.0, 0.0, 0.0, 0.0, 0.0, 0.0, 80.0, 1013.0, 50.0, 10000.0),
        ("DK2", 2023, 1, 1, 12, 6.0, 12.0, 5, 7.0, 5.0, 0.0, 0.0, 14.0, 0.5, 0.5, 16.0, 0.0, 0.0, 0.0, 0.0, 0.0, 82.0, 1012.0, 60.0, 9000.0),
        ("DK1", 2024, 1, 1, 12, 4.0, 8.0, 5, 5.0, 3.0, 0.0, 0.0, 10.0, 0.5, 0.5, 12.0, 0.0, 0.0, 0.0, 0.0, 0.0, 75.0, 1015.0, 40.0, 12000.0),
        ("DK1", 2025, 1, 1, 12, 3.0, 15.0, 5, 4.0, 2.0, 0.0, 0.0, 18.0, 0.5, 0.5, 20.0, 0.0, 0.0, 0.0, 0.0, 0.0, 85.0, 1010.0, 80.0, 8000.0), # Validation
        ("DK1", 2025, 11, 1, 12, 2.0, 20.0, 5, 3.0, 1.0, 0.0, 0.0, 25.0, 0.5, 0.5, 28.0, 0.0, 0.0, 0.0, 0.0, 0.0, 90.0, 1005.0, 90.0, 5000.0)  # Test
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def mock_consumption_data(spark):
    """Create mock consumption data"""
    schema = StructType([
        StructField("dk_area", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("consumption_mwh_area", DoubleType(), True)
    ])
    
    data = [
        ("DK1", 2023, 1, 1, 12, 1000.0),
        ("DK2", 2023, 1, 1, 12, 1200.0),
        ("DK1", 2024, 1, 1, 12, 1100.0),
        ("DK1", 2025, 1, 1, 12, 1050.0), # Validation
        ("DK1", 2025, 11, 1, 12, 1300.0) # Test
    ]
    
    return spark.createDataFrame(data, schema)

def test_split_data(spark, mock_weather_data, mock_consumption_data):
    """Test data splitting logic"""
    # Join data first (simulating load_and_join_data)
    joined_df = mock_weather_data.join(
        mock_consumption_data,
        on=['dk_area', 'year', 'month', 'day', 'hour'],
        how='inner'
    )
    
    train_df, val_df, test_df = train_consumption_model.split_data(joined_df)
    
    # Check counts based on the mock data years/months
    # Train: 2023, 2024 (3 records)
    # Val: 2025 Jan (1 record)
    # Test: 2025 Nov (1 record)
    
    assert train_df.count() == 3
    assert val_df.count() == 1
    assert test_df.count() == 1
    
    # Verify date ranges
    assert train_df.filter(train_df.year > 2024).count() == 0
    assert val_df.filter(val_df.year != 2025).count() == 0
    assert test_df.filter((test_df.year == 2025) & (test_df.month == 11)).count() == 1

def test_build_and_train_model(spark, mock_weather_data, mock_consumption_data):
    """Test model training pipeline"""
    joined_df = mock_weather_data.join(
        mock_consumption_data,
        on=['dk_area', 'year', 'month', 'day', 'hour'],
        how='inner'
    )
    
    train_df, val_df, test_df = train_consumption_model.split_data(joined_df)
    
    # Train model
    model = train_consumption_model.build_and_train_model(train_df, val_df)
    
    assert model is not None
    
    # Test prediction
    predictions = model.transform(test_df)
    assert "prediction" in predictions.columns
    
    # Check that predictions are reasonable (not null)
    row = predictions.select("prediction").first()
    assert row["prediction"] is not None
    assert row["prediction"] > 0

def test_evaluate_model(spark, mock_weather_data, mock_consumption_data):
    """Test model evaluation"""
    joined_df = mock_weather_data.join(
        mock_consumption_data,
        on=['dk_area', 'year', 'month', 'day', 'hour'],
        how='inner'
    )
    
    train_df, val_df, test_df = train_consumption_model.split_data(joined_df)
    model = train_consumption_model.build_and_train_model(train_df, val_df)
    
    rmse, r2 = train_consumption_model.evaluate_model(model, test_df)
    
    assert isinstance(rmse, float)
    assert isinstance(r2, float)
