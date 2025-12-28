import os
import json
import datetime
import random
import sys
from typing import List

from pyspark.sql import SparkSession, functions as F, types as T, Row
from pyspark.sql.window import Window
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, Imputer, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

CONFIG = {
    "HIVE_DB" : "default",
    "HIVE_TABLE" : "ml_training_data",  # ML dataset table
    "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",  # Hive metastore connection

    "MODEL_OUTPUT_PATH" : "./SparkML/models",
    "MODEL_NAME" : "energy_model",

    # Weather features (inputs)
    "WEATHER_FEATURES": [
        "wind_speed_avg",
        "wind_speed_max",
        "wind_direction_sin",
        "wind_direction_cos",
        "solar_radiation_1h",
        "sunshine_duration_1h",
        "cloud_cover_avg",
        "n_stations_wind",
        "n_stations_solar"
    ],

    # Temporal features (inputs)
    "TEMPORAL_FEATURES": [
        "month_of_year",
        "hour_of_day"
    ],

    # Categorical features
    "CATEGORICAL_FEATURES": ["dk_area"],

    # Target variables (outputs)
    "PRODUCTION_TARGET": "total_production_mwh",
    "CONSUMPTION_TARGET": "total_consumption_mwh",

    # Training parameters
    "TRAIN_START_YEAR": 2021,
    "TRAIN_END_YEAR": 2024,
    "VAL_YEAR": 2025,
    "VAL_MONTHS": [1,2,3,4,5,6,7,8,9,10],  # Jan-Oct
    "TEST_YEAR": 2025,
    "TEST_MONTH": 11,  # November

    "TOKEN_SIZE" : 8,  # Keep for backward compatibility
    "LAGS": [1, 2, 24],
    "ROLL_WINDOWS_HOURS": [3, 24, 168],

    # Model hyperparams (search space)
    "RF": {"numTrees": [50, 100], "maxDepth": [6, 10]},  # Production settings
    "GBT": {"maxIter": [50, 100], "maxDepth": [4, 8]},  # Production settings

    "SPARK_MASTER" : "local[*]",
    "APP_NAME" : "EnergyPredictionML",

    "SAMPLE_ZONE": None,
    "USE_MOCK_DATA": False,  # Set to True to test with mock data
}



def now_tag():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")


def create_spark_session():

    # Stop any existing Spark session first to clear any cached Connect settings
    try:
        from pyspark.sql import SparkSession as SS
        existing_spark = SS.getActiveSession()
        if existing_spark is not None:
            print("Stopping existing Spark session...")
            existing_spark.stop()
    except:
        pass

    # Force disable Spark Connect by unsetting the environment variable
    # and ensuring we use the traditional JVM-based Spark
    # Remove both the variable and any empty string values
    # Empty strings are treated as valid Connect URLs by PySpark, so we must delete them
    if "SPARK_REMOTE" in os.environ:
        del os.environ["SPARK_REMOTE"]
        print("Removed SPARK_REMOTE environment variable")
    
    if "SPARK_CONNECT_MODE_ENABLED" in os.environ:
        del os.environ["SPARK_CONNECT_MODE_ENABLED"]
        print("Removed SPARK_CONNECT_MODE_ENABLED environment variable")
    
    # Check PySpark version - newer versions default to Connect
    try:
        import pyspark
        pyspark_version = pyspark.__version__
        print(f"PySpark version: {pyspark_version}")
    except:
        pass
    
    # Create session with explicit configs to avoid Connect
    # IMPORTANT: Set Spark Connect disable configs BEFORE setting master
    # to prevent PySpark from detecting Connect URLs
    builder = SparkSession.builder \
        .appName(CONFIG["APP_NAME"]) \
        .config("spark.sql.connect.enabled", "false") \
        .config("spark.sql.connect.server.enabled", "false") \
        .config("spark.sql.shuffle.partitions", 8) \
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .master(CONFIG["SPARK_MASTER"])

    # Add Hive support if metastore URI is configured
    if not CONFIG["USE_MOCK_DATA"] and CONFIG.get("HIVE_METASTORE_URI"):
        builder = builder.config("hive.metastore.uris", CONFIG["HIVE_METASTORE_URI"]) \
                        .enableHiveSupport()
        print(f"Hive metastore configured: {CONFIG['HIVE_METASTORE_URI']}")

    spark = builder.getOrCreate()

    # Check if we're in Connect mode (Connect sessions don't have sparkContext)
    if not hasattr(spark, 'sparkContext'):
        print("ERROR: Spark Connect mode detected. Attempting to fix...")
        # Try to stop and recreate
        try:
            spark.stop()
        except:
            pass
        
        # Force create a new session without Connect
        spark = SparkSession.builder \
            .appName(CONFIG["APP_NAME"]) \
            .master(CONFIG["SPARK_MASTER"]) \
            .config("spark.sql.shuffle.partitions", 8) \
            .getOrCreate()
        
        # Check again
        if not hasattr(spark, 'sparkContext'):
            raise RuntimeError(
                "\n" + "="*60 + "\n"
                "ERROR: PySpark is using Spark Connect mode, which doesn't support MLlib.\n"
                "To fix this, run in WSL:\n"
                "  pip3 uninstall pyspark\n"
                "  pip3 install 'pyspark==3.5.0'\n"
                "Or set: export SPARK_CONNECT_MODE_ENABLED=false\n"
                "="*60
            )
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session created (JVM mode)")
    return spark

# ---- Mock Data ----
def create_mock_data(spark, num_rows=1000):
    """
    Generates a DataFrame with columns: ts, zone, price, token
    to mimic the expected input for testing.
    """
    print(f"--- Generating {num_rows} rows of mock data ---")
    
    data = []
    base_time = datetime.datetime.now() - datetime.timedelta(days=60)
    zones = ["ZoneA", "ZoneB"]
    
    for i in range(num_rows):
        # Create a linear time sequence with some randomness
        row_time = base_time + datetime.timedelta(hours=i % (num_rows//2))
        
        # Random data
        zone = zones[i % 2]
        price = 50.0 + (i % 24) + random.uniform(-5, 5) # Periodic price pattern
        
        # Token array (mocking weather/embedding data)
        token = [random.random() for _ in range(CONFIG["TOKEN_SIZE"])]
        
        data.append(Row(
            ts=row_time,
            zone=zone,
            price=float(price),
            token=token 
        ))
        
    schema = T.StructType([
        T.StructField("ts", T.TimestampType(), True),
        T.StructField("zone", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("token", T.ArrayType(T.DoubleType()), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    return df

# ---- Load Data -----

def load_hive_data(spark, use_mock=False):

    if use_mock:
        df = create_mock_data(spark, num_rows=2000)

        # Register as Temp View so SQL queries in score_live_weather work
        # We handle the "database.table" naming by registering it strictly as the table name
        # and adjusting the query logic slightly or relying on Spark resolving it.
        df.createOrReplaceTempView(CONFIG["HIVE_TABLE"])
        return df
    else:
        # Read from Hive ml_training_data view
        print("Loading ML dataset from Hive ml_training_data view...")

        # Ensure we're using the default database
        spark.sql("USE default")

        # Check if table exists
        tables = spark.sql("SHOW TABLES").collect()
        print(f"  Available tables: {[row.tableName for row in tables]}")

        # Try to read the view
        try:
            df = spark.sql("SELECT * FROM default.ml_training_data")
            row_count = df.count()
            print(f"  ✓ Loaded ML dataset with {row_count:,} rows from Hive view")
            return df
        except Exception as e:
            print(f"  ✗ Failed to read from Hive view: {e}")
            print(f"  Falling back to direct Parquet read...")

            # Fallback: read directly from HDFS
            weather_df = spark.read.parquet("hdfs://namenode:9000/user/hive/warehouse/weather_wind_solar_area_hourly")
            production_df = spark.read.parquet("hdfs://namenode:9000/user/hive/warehouse/energy_by_municipality")
            consumption_df = spark.read.parquet("hdfs://namenode:9000/user/hive/warehouse/consumption_coverage_location")

            # Register as temp views
            weather_df.createOrReplaceTempView("weather")
            production_df.createOrReplaceTempView("production")
            consumption_df.createOrReplaceTempView("consumption")

            # Join manually
            df = spark.sql("""
                SELECT
                  w.year, w.month, w.day, w.hour, w.dk_area,
                  w.month as month_of_year,
                  w.hour as hour_of_day,
                  w.wind_speed_mean_area as wind_speed_avg,
                  w.wind_speed_max_area as wind_speed_max,
                  w.wind_dir_sin_area as wind_direction_sin,
                  w.wind_dir_cos_area as wind_direction_cos,
                  w.radia_glob_past1h_area as solar_radiation_1h,
                  w.sun_last1h_glob_area as sunshine_duration_1h,
                  w.cloud_cover_mean_area as cloud_cover_avg,
                  w.n_stations_wind,
                  w.n_stations_solar,
                  p.total_production_mwh,
                  c.total_consumption_mwh
                FROM weather w
                LEFT JOIN (
                  SELECT
                    CASE WHEN CAST(MunicipalityNo AS INT) < 400 THEN 'DK1' ELSE 'DK2' END as dk_area,
                    year, month, day, hour,
                    SUM(
                      COALESCE(SolarMWh, 0) +
                      COALESCE(OffshoreWindLt100MW_MWh, 0) +
                      COALESCE(OffshoreWindGe100MW_MWh, 0) +
                      COALESCE(OnshoreWindMWh, 0) +
                      COALESCE(ThermalPowerMWh, 0)
                    ) as total_production_mwh
                  FROM production
                  GROUP BY
                    CASE WHEN CAST(MunicipalityNo AS INT) < 400 THEN 'DK1' ELSE 'DK2' END,
                    year, month, day, hour
                ) p ON w.dk_area = p.dk_area
                   AND w.year = p.year
                   AND w.month = p.month
                   AND w.day = p.day
                   AND w.hour = p.hour
                LEFT JOIN (
                  SELECT PriceArea as dk_area, year, month, day, hour,
                         SUM(ShareMWh) as total_consumption_mwh
                  FROM consumption
                  GROUP BY PriceArea, year, month, day, hour
                ) c ON w.dk_area = c.dk_area
                   AND w.year = c.year
                   AND w.month = c.month
                   AND w.day = c.day
                   AND w.hour = c.hour
            """)

            row_count = df.count()
            print(f"  ✓ Fallback successful: {row_count:,} rows")
            return df


#----- Feature Engineering -----

def expand_token(df, token_col="token", token_size=CONFIG["TOKEN_SIZE"]):
    """
    Expand array<double> token into individual numeric columns token_0..token_{n-1}.
    If token is shorter than token_size, missing entries become null.
    """
    for i in range(token_size):
        df = df.withColumn(f"token_{i}", F.col(token_col).getItem(i).cast("double"))
    return df


def build_time_feature(df, ts_col="ts"):
    """
    Add epoch, hour of day, day of week, month, is_weekend columns
    Can adjust accordingly to features needed.
    Feature nedeed because Spark cannot understand timestamp type directly.
    """
    df = df.withColumn("Epoch", F.col(ts_col).cast("long"))
    df = df.withColumn("HourOfDay", F.hour(ts_col))
    # Use built-in dayofweek instead (1=Sun, 7=Sat) and adjust weekend logic if needed.
    df = df.withColumn("DayOfWeek", F.dayofweek(ts_col))
    df = df.withColumn("Month", F.month(ts_col))
    df = df.withColumn("is_weekend", (F.col("DayOfWeek").isin([7,1])).cast("int"))
    return df


def add_lag_and_rolling_features(df, zone_col="zone", epoch_col="Epoch", price_col="price", lags=[1,2], roll_windows=[24]):
    """
    Add lag and rolling window features based on CONFIG settings.
    Add lag features (lag_{h}) and rolling mean features (roll_{window}h)
    Expects hourly data; lags/roll windows provided in hours.
    """

    w_zone = Window.partitionBy(zone_col).orderBy(epoch_col)
    for h in lags:
        df = df.withColumn(f"price_lag_{h}", F.lag(price_col, h).over(w_zone))
    
    # Rolling mean using rangeBetwqeen in seconds
    for w in roll_windows:
        seconds = w * 3600
        w_roll = Window.partitionBy(zone_col).orderBy(epoch_col).rangeBetween(-seconds, 0)
        df = df.withColumn(f"price_roll_mean_{w}h", F.avg(price_col).over(w_roll)) # Rolling mean
        df = df.withColumn(f"price_roll_std_{w}h", F.stddev(price_col).over(w_roll)) # Rolling standard deviation
    
    return df

def prepare_features(df, use_mock=False):
    """
    Complete feature engineering pipeline on the DataFrame received

    For mock data: uses old token-based features
    For real ML data: uses weather + temporal features from ml_training_data
    """
    if use_mock:
        # Old token-based feature engineering for mock data
        df = expand_token(df, token_size=CONFIG["TOKEN_SIZE"])
        df = build_time_feature(df, "ts")
        df = add_lag_and_rolling_features(df,
                                          epoch_col="Epoch",
                                          lags=CONFIG["LAGS"],
                                          roll_windows=CONFIG["ROLL_WINDOWS_HOURS"])

        feature_cols = ["Demand", "HourOfDay", "DayOfWeek", "Month", "is_weekend"]

        for h in CONFIG["LAGS"]:
            feature_cols.append(f"price_lag_{h}")

        for w in CONFIG["ROLL_WINDOWS_HOURS"]:
            feature_cols.append(f"price_roll_mean_{w}h")
            feature_cols.append(f"price_roll_std_{w}h")

        for i in range(CONFIG["TOKEN_SIZE"]):
            feature_cols.append(f"token_{i}")

        df = df.filter(F.col("price").isNotNull())

        if "Demand" not in df.columns:
            if "Demand" in feature_cols: feature_cols.remove("Demand")

    else:
        # New ML dataset features (weather + temporal)
        feature_cols = CONFIG["WEATHER_FEATURES"] + CONFIG["TEMPORAL_FEATURES"]

        # Filter rows with null targets
        df = df.filter(
            F.col(CONFIG["PRODUCTION_TARGET"]).isNotNull() &
            F.col(CONFIG["CONSUMPTION_TARGET"]).isNotNull()
        )

        print(f"\nFeatures ({len(feature_cols)}):")
        for feat in feature_cols:
            print(f"  - {feat}")
        print(f"Categorical: {CONFIG['CATEGORICAL_FEATURES']}")
        print(f"Target 1: {CONFIG['PRODUCTION_TARGET']}")
        print(f"Target 2: {CONFIG['CONSUMPTION_TARGET']}")

    return df, feature_cols

#----- Train/Validate/Test by time split -----
def time_based_split(df, use_mock=False):
    """
    Split DataFrame into train/validation/test using time-based splits.
    Ensures temporal split (no leakage)

    For mock data: uses percentiles on epoch
    For ML data: uses year/month from CONFIG
    """
    if use_mock:
        # Old percentile-based split for mock data
        epoch_col = "Epoch"
        fractions = [CONFIG.get("TRAIN_FRAC", 0.7), CONFIG.get("VALID_FRAC", 0.15), CONFIG.get("TEST_FRAC", 0.15)]
        assert abs(sum(fractions) - 1.0) < 0.01, "Train/Valid/Test fractions must sum to 1.0"

        percentiles = df.approxQuantile(epoch_col, [fractions[0], fractions[0] + fractions[1]], 0.01)
        p1, p2 = percentiles[0], percentiles[1]

        train = df.filter(F.col(epoch_col) <= p1)
        val = df.filter((F.col(epoch_col) > p1) & (F.col(epoch_col) <= p2))
        test = df.filter(F.col(epoch_col) > p2)

        return train, val, test, p1, p2
    else:
        # New time-based split for ML dataset
        # Training: 2021-2024
        train = df.filter(
            (F.col("year") >= CONFIG["TRAIN_START_YEAR"]) &
            (F.col("year") <= CONFIG["TRAIN_END_YEAR"])
        )

        # Validation: Jan-Oct 2025
        val = df.filter(
            (F.col("year") == CONFIG["VAL_YEAR"]) &
            (F.col("month").isin(CONFIG["VAL_MONTHS"]))
        )

        # Test: Nov 2025
        test = df.filter(
            (F.col("year") == CONFIG["TEST_YEAR"]) &
            (F.col("month") == CONFIG["TEST_MONTH"])
        )

        print(f"\nTime-based splits:")
        print(f"  Train: {CONFIG['TRAIN_START_YEAR']}-{CONFIG['TRAIN_END_YEAR']}")
        print(f"  Val: {CONFIG['VAL_YEAR']} months {CONFIG['VAL_MONTHS']}")
        print(f"  Test: {CONFIG['TEST_YEAR']}-{CONFIG['TEST_MONTH']:02d}")

        return train, val, test, None, None

#----- Spark Pipeline ------
def build_pipeline(feature_cols: List[str], label_col="price", model_type="RF", include_categorical=False):
    """Return a Pipeline instance with Imputer, Assembler, Scaler, and estimator placeholder

    Args:
        feature_cols: List of numeric feature column names
        label_col: Target variable column name
        model_type: "RF" or "GBT"
        include_categorical: If True, adds StringIndexer + OneHotEncoder for dk_area
    """
    from pyspark.ml.feature import StringIndexer, OneHotEncoder

    stages = []
    assembler_cols = feature_cols.copy()

    # Handle categorical feature (dk_area) if needed
    if include_categorical and "dk_area" in CONFIG.get("CATEGORICAL_FEATURES", []):
        indexer = StringIndexer(
            inputCol="dk_area",
            outputCol="dk_area_indexed",
            handleInvalid="keep"
        )
        encoder = OneHotEncoder(
            inputCols=["dk_area_indexed"],
            outputCols=["dk_area_encoded"]
        )
        stages.extend([indexer, encoder])
        assembler_cols.append("dk_area_encoded")

    # Impute missing values
    imputer = Imputer(inputCols=feature_cols, outputCols=feature_cols, strategy="mean")
    stages.append(imputer)

    # Assemble features
    assembler = VectorAssembler(inputCols=assembler_cols, outputCol="features_raw", handleInvalid="skip")
    stages.append(assembler)

    # Scale features
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    stages.append(scaler)

    # Add estimator
    if model_type == "RF":
        estimator = RandomForestRegressor(featuresCol="features", labelCol=label_col,
                                         predictionCol="prediction", seed=42)
    elif model_type == "GBT":
        estimator = GBTRegressor(featuresCol="features", labelCol=label_col,
                                predictionCol="prediction", seed=42)
    else:
        raise ValueError("Unsupported model type. Choose 'RF' or 'GBT'.")

    stages.append(estimator)
    pipeline = Pipeline(stages=stages)

    return pipeline, estimator

# ---- Hyperparameter Tuning -----

def time_aware_train_val(pipeline, estimator, train_df, val_df, parm_grid, evaluator, parallelism=2):
    """
    Perform time-aware train-validation split using provided 
    train and validation DataFrames.
    """
    
    union = train_df.unionByName(val_df)
    train_size = train_df.count()
    total = union.count()
    train_ratio = float(train_size) / float(total) if total > 0 else 0.8

    tvs = TrainValidationSplit(estimator=pipeline,
                                estimatorParamMaps=parm_grid,
                                evaluator=evaluator,
                                trainRatio=train_ratio,
                                parallelism=parallelism)
    print(f"Starting Train-Validation Split with train ratio: {train_ratio:.4f}")

    tvs_model = tvs.fit(union)
    return tvs_model

# --- Evaluation -----
def evaluate_model(model_pipeline, df, evaluator):
    predictions = model_pipeline.transform(df)
    rmse = evaluator.evaluate(predictions)

    # Get the label column from the evaluator
    label_col = evaluator.getLabelCol()

    mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="r2").evaluate(predictions)

    return {"RMSE": rmse, "MAE": mae, "R2": r2}

# ---- Save model and the Metadata -----

def save_model_and_metadata(model_pipeline, metadata: dict, base_dir: str, mode_name: str):
    tag = now_tag()
    model_path = os.path.join(base_dir, f"{mode_name}_{tag}")
    metadata_path = os.path.join(model_path, "metadata.json")

    model_pipeline.write().overwrite().save(model_path)

    os.makedirs(model_path, exist_ok=True)
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Model saved to {model_path}")
    return model_path

# ----- Scoring -----
def score_live_with_object(spark, model_pipeline, live_weather_token: List[float], zone: str, use_mock=False):
    """
    Modified scoring function that takes the model OBJECT instead of path, 
    so we can test immediately without reloading from disk.
    """
    table_name = CONFIG["HIVE_TABLE"]

    # If using mock, the view is just 'sample_table', no DB prefix
    query_table = table_name if use_mock else f"{CONFIG['HIVE_DB']}.{table_name}"

    print(f"Querying recent history from: {query_table} for zone {zone}")
    
    if zone:
        recent = spark.sql(f"SELECT * FROM {query_table} WHERE zone = '{zone}' ORDER BY ts DESC LIMIT 200")
    else:
        recent = spark.sql(f"SELECT * FROM {query_table} ORDER BY ts DESC LIMIT 200")

    if dict(recent.dtypes)["ts"] == 'string':
        recent = recent.withColumn("ts", F.to_timestamp("ts"))

     # Ensure there is data
    if recent.count() == 0:
        print("No recent data found for scoring context.")
        return None

    recent = expand_token(recent, token_size=CONFIG["TOKEN_SIZE"])
    recent = build_time_feature(recent, "ts")
    recent = add_lag_and_rolling_features(recent,
                                          epoch_col="Epoch",
                                          lags=CONFIG["LAGS"],
                                          roll_windows=CONFIG["ROLL_WINDOWS_HOURS"])
    
    # Get the latest row as baseline for lags
    #baseline = recent.orderBy(F.col("Epoch").desc()).limit(1).toPandas().to_dict(orient="records")[0]
    
    # FIX: Cast 'ts' to string to avoid Pandas "unit-less datetime64" error
    recent_for_pandas = recent.withColumn("ts", F.col("ts").cast("string"))
    
    baseline = recent_for_pandas.orderBy(F.col("Epoch").desc()).limit(1).toPandas().to_dict(orient="records")[0]


    feature_data = {}
    feature_data["Demand"] = float(baseline.get("Demand") or 0.0)

    now_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    dt = datetime.datetime.fromtimestamp(now_ts, datetime.timezone.utc)
    feature_data["HourOfDay"] = dt.hour
    feature_data["DayOfWeek"] = int(dt.isoweekday())
    feature_data["Month"] = dt.month
    feature_data["is_weekend"] = 1 if feature_data["DayOfWeek"] in [6,7] else 0

    for h in CONFIG["LAGS"]:
        val = baseline.get(f"price_lag_{h}")
        if val is None: val = baseline.get("price") # fallback
        feature_data[f"price_lag_{h}"] = float(val or 0.0)
    
    for w in CONFIG["ROLL_WINDOWS_HOURS"]:
        feature_data[f"price_roll_mean_{w}h"] = float(baseline.get(f"price_roll_mean_{w}h") or baseline.get("price") or 0.0)
        feature_data[f"price_roll_std_{w}h"] = float(baseline.get(f"price_roll_std_{w}h") or 0.0)
        
    # Token features (Live Input)
    token = list(live_weather_token or [])
    # Pad if short
    if len(token) < CONFIG["TOKEN_SIZE"]:
        token = token + [0.0] * (CONFIG["TOKEN_SIZE"] - len(token))
        
    for i in range(CONFIG["TOKEN_SIZE"]):
        feature_data[f"token_{i}"] = float(token[i])

    feature_df = spark.createDataFrame([feature_data])
    
    # Fill any missing columns (like Demand if not in feature_data) with 0
    # The VectorAssembler needs all input columns to exist.
    # We check what the model expects (metadata from training usually)
    # Here we just try/catch the transform
    try:
        prediction = model_pipeline.transform(feature_df)
        return prediction.select("prediction").toPandas()
    except Exception as e:
        print(f"Scoring failed: {e}")
        return None

# ----- MAIN!!!!! -----

def main():

    USE_MOCK_DATA = CONFIG["USE_MOCK_DATA"]  # Use from CONFIG

    print("\n" + "="*70)
    print("ENERGY PREDICTION MODEL TRAINING")
    print("="*70)
    print(f"Mode: {'MOCK DATA (testing)' if USE_MOCK_DATA else 'REAL ML DATASET'}")

    spark = create_spark_session()

    try:
        # Load data
        df = load_hive_data(spark, use_mock=USE_MOCK_DATA)
        print(f"\n✓ Loaded {df.count():,} total rows")

        # Prepare features
        df_features, feature_cols = prepare_features(df, use_mock=USE_MOCK_DATA)

        # Split data
        train, val, test, p1, p2 = time_based_split(df_features, use_mock=USE_MOCK_DATA)

        if USE_MOCK_DATA:
            print(f"\nSplit epochs: split1={p1}, split2={p2}")

        train_count = train.count()
        val_count = val.count()
        test_count = test.count()
        print(f"\nData splits:")
        print(f"  Train: {train_count:,} records")
        print(f"  Val:   {val_count:,} records")
        print(f"  Test:  {test_count:,} records")

        # Train production and consumption models
        print("\n" + "="*70)
        print("TRAINING PRODUCTION MODEL")
        print("="*70)

        prod_pipeline, prod_estimator = build_pipeline(
            feature_cols,
            label_col=CONFIG["PRODUCTION_TARGET"],
            model_type="RF",
            include_categorical=True
        )

        prod_param_grid = ParamGridBuilder() \
            .addGrid(prod_estimator.numTrees, CONFIG["RF"]["numTrees"]) \
            .addGrid(prod_estimator.maxDepth, CONFIG["RF"]["maxDepth"]) \
            .build()

        prod_evaluator = RegressionEvaluator(
            labelCol=CONFIG["PRODUCTION_TARGET"],
            predictionCol="prediction",
            metricName="rmse"
        )

        print(f"\nTraining with {len(prod_param_grid)} hyperparameter combinations...")
        prod_tvs_model = time_aware_train_val(prod_pipeline, prod_estimator, train, val,
                                               prod_param_grid, prod_evaluator, parallelism=2)

        prod_metrics = evaluate_model(prod_tvs_model.bestModel, test, prod_evaluator)
        print(f"\n✓ Production Model Test Metrics: {prod_metrics}")

        # Save production model
        prod_metadata = {
            "created_at": now_tag(),
            "target": CONFIG["PRODUCTION_TARGET"],
            "model_type": "RandomForest",
            "features": feature_cols,
            "categorical_features": CONFIG["CATEGORICAL_FEATURES"],
            "test_metrics": prod_metrics,
            "config": CONFIG
        }
        prod_model_path = save_model_and_metadata(
            prod_tvs_model.bestModel,
            prod_metadata,
            CONFIG["MODEL_OUTPUT_PATH"],
            f"{CONFIG['MODEL_NAME']}_production"
        )

        print("\n" + "="*70)
        print("TRAINING CONSUMPTION MODEL")
        print("="*70)

        cons_pipeline, cons_estimator = build_pipeline(
            feature_cols,
            label_col=CONFIG["CONSUMPTION_TARGET"],
            model_type="RF",
            include_categorical=True
        )

        cons_param_grid = ParamGridBuilder() \
            .addGrid(cons_estimator.numTrees, CONFIG["RF"]["numTrees"]) \
            .addGrid(cons_estimator.maxDepth, CONFIG["RF"]["maxDepth"]) \
            .build()

        cons_evaluator = RegressionEvaluator(
            labelCol=CONFIG["CONSUMPTION_TARGET"],
            predictionCol="prediction",
            metricName="rmse"
        )

        print(f"\nTraining with {len(cons_param_grid)} hyperparameter combinations...")
        cons_tvs_model = time_aware_train_val(cons_pipeline, cons_estimator, train, val,
                                               cons_param_grid, cons_evaluator, parallelism=2)

        cons_metrics = evaluate_model(cons_tvs_model.bestModel, test, cons_evaluator)
        print(f"\n✓ Consumption Model Test Metrics: {cons_metrics}")

        # Save consumption model
        cons_metadata = {
            "created_at": now_tag(),
            "target": CONFIG["CONSUMPTION_TARGET"],
            "model_type": "RandomForest",
            "features": feature_cols,
            "categorical_features": CONFIG["CATEGORICAL_FEATURES"],
            "test_metrics": cons_metrics,
            "config": CONFIG
        }
        cons_model_path = save_model_and_metadata(
            cons_tvs_model.bestModel,
            cons_metadata,
            CONFIG["MODEL_OUTPUT_PATH"],
            f"{CONFIG['MODEL_NAME']}_consumption"
        )

        print("\n" + "="*70)
        print("TRAINING COMPLETE")
        print("="*70)
        print(f"✓ Production model: {prod_model_path}")
        print(f"✓ Consumption model: {cons_model_path}")
        print(f"\n✓ Models ready for prediction!")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()