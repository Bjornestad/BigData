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
    "HIVE_TABLE" : "sample_table",

    "MODEL_OUTPUT_PATH" : "./SparkML/models", 
    "MODEL_NAME" : "spark_model",

    "TOKEN_SIZE" : 8, 
    
    "TRAIN_FRAC" : 0.7,
    "VALID_FRAC" : 0.15,
    "TEST_FRAC" : 0.15,

    # Time-series feature settings (hours)
    "LAGS": [1, 2, 24],        # lag in hours
    "ROLL_WINDOWS_HOURS": [3, 24, 168],  # rolling windows in hours (3hr, 24hr, 7d)

    # Model hyperparams (search space) -- Reduced for faster mock testing
    "RF": {"numTrees": [10, 20], "maxDepth": [5]}, # Set numTrees to [50, 100] and maxDepth to [6,10] for real runs 
    "GBT": {"maxIter": [10], "maxDepth": [4]}, # Set maxIter to [50, 100] and maxDepth to [4,8] for real runs

    "SPARK_MASTER" : "local[*]",
    "APP_NAME" : "SparkApp",

    "SAMPLE_ZONE": None,
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
    spark = ( SparkSession.builder \
    .appName(CONFIG["APP_NAME"]) \
    .config("spark.sql.connect.enabled", "false") \
    .config("spark.sql.connect.server.enabled", "false") \
    .config("spark.sql.shuffle.partitions", 8) \
    .master(CONFIG["SPARK_MASTER"]) \
    .getOrCreate()
    )
    """
    .enableHiveSupport() \
    """

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
        db = CONFIG["HIVE_DB"]
        table = CONFIG["HIVE_TABLE"]
        sql = f"SELECT * FROM {db}.{table}" # Edit * to select specific columns if needed
        df = spark.sql(sql)

        #Ensure correct schema and types
        df = df.withColumn("ts", F.to_timestamp("ts"))
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

def prepare_features(df):
    """
    Complete feature engineering pipelin on the Dataframe recieved
    """
    df = expand_token(df, token_size=CONFIG["TOKEN_SIZE"])

    df = build_time_feature(df, "ts")

    df = add_lag_and_rolling_features(df,
                                      epoch_col="Epoch",
                                      lags=CONFIG["LAGS"],
                                      roll_windows=CONFIG["ROLL_WINDOWS_HOURS"])
    
    # Create feature column list (dropping identifiers)
    # Numeric features from example: demand, lags, rollings, time-of-day, weather tokens

    feature_cols = ["Demand", "HourOfDay", "DayOfWeek", "Month", "is_weekend"]

    # adding lags
    for h in CONFIG["LAGS"]:
        feature_cols.append(f"price_lag_{h}")

    # add rolling stats
    for w in CONFIG["ROLL_WINDOWS_HOURS"]:
        feature_cols.append(f"price_roll_mean_{w}h")
        feature_cols.append(f"price_roll_std_{w}h")

    # add Token columns
    for i in range(CONFIG["TOKEN_SIZE"]):
        feature_cols.append(f"token_{i}")
    
    # Drop rows with null target
    df = df.filter(F.col("price").isNotNull())  
    
    if "Demand" not in df.columns:
        if "Demand" in feature_cols: feature_cols.remove("Demand")
    
    return df, feature_cols

#----- Train/Validate/Test by time split -----
def time_based_split(df, epoch_col="Epoch"):
    """
    Split DataFrame into train/validation/test using percentiles on epoch time.
    Ensures temporal split (no leakage)
    """
    fractions = [CONFIG["TRAIN_FRAC"], CONFIG["VALID_FRAC"], CONFIG["TEST_FRAC"]]
    assert sum(fractions) == 1.0, "Train/Valid/Test fractions must sum to 1.0"
    
    # compute split epochs: 70th and 85th percentiles for example (train up to p1, val p1 -> p2, test > p2)
    percentiles = df.approxQuantile(epoch_col, [CONFIG["TRAIN_FRAC"], CONFIG["TRAIN_FRAC"] + CONFIG["VALID_FRAC"]], 0.01)
    p1, p2 = percentiles[0], percentiles[1]
    
    train = df.filter(F.col(epoch_col) <= p1)
    val = df.filter((F.col(epoch_col) > p1) & (F.col(epoch_col) <= p2))
    test = df.filter(F.col(epoch_col) > p2)
    
    return train, val, test, p1, p2

#----- Spark Pipeline ------
def build_pipeline(feature_cols: List[str], label_col="price", model_type="RF"):
    """Return a Pipeline instance with Imputer, Assembler, Scaler, and estimator placeholder"""

    imputer = Imputer(inputCols=feature_cols, outputCols=feature_cols)
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="keep")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

    if model_type == "RF":
        estimator = RandomForestRegressor(featuresCol="features", labelCol=label_col, predictionCol="prediction")
    elif model_type == "GBT":
        estimator = GBTRegressor(featuresCol="features", labelCol=label_col, predictionCol="prediction")
    else:
        raise ValueError("Unsupported model type. Choose 'RF' or 'GBT'.")
    
    pipeline = Pipeline(stages=[imputer, assembler, scaler, estimator])
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
    mae = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="mae").evaluate(predictions)
    r2 = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2").evaluate(predictions)

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
def score_live_weather(spark, model_path: str, live_weather_token: List[float], zone: str, use_mock=False):
    """
    Score a live weather token by building required features:
    - fetch most recent row(s) for the zone to compute lags/rolling stats
    - build a single-row DataFrame with assembled features
    - load model and predict
    """
    """ 
    model = PipelineModel.load(model_path)

    table = f"{CONFIG["HIVE_DB"]}.{CONFIG["HIVE_TABLE"]}"
    if zone:
        recent = spark.sql(f"SELECT * FROM {table} WHERE zone = '{zone}' ORDER BY ts DESC LIMIT 200")
    else:
        recent = spark.sql(f"SELECT * FROM {table} ORDER BY ts DESC LIMIT 200")

    recent = recent.withColumn("ts", F.to_timestamp("ts"))
    recent = recent.withColumn("ts_epoch", F.col("ts").cast("long"))

    if recent.count() == 0:
        raise ValueError("No recent rows found for scoring.")
    
    recent = expand_token(recent, token_size=CONFIG["TOKEN_SIZE"])
    recent = build_time_feature(recent, "ts")
    recent = add_lag_and_rolling_features(recent,
                                          lags=CONFIG["LAGS"],
                                          roll_windows=CONFIG["ROLL_WINDOWS_HOURS"])
    
    baseline = recent.orderBy(F.col("ts_epoch").desc()).limit(1).toPandas().to_dict(orient="records")[0]
    #Feature dictionary
    feature_data = {}
    
    feature_data["Demand"] = float(baseline.get("Demand") or 0.0)

    now_ts =int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    dt = datetime.datetime.fromtimestamp(now_ts, datetime.timezone.utc)
    feature_data["HourOfDay"] = dt.hour
    feature_data["DayOfWeek"] = int(dt.isoweekday())
    feature_data["Month"] = dt.month
    feature_data["is_weekend"] = 1 if feature_data["DayOfWeek"] in [6,7] else 0

    #Lags: Use baseline values or fallback to price
    for h in CONFIG["LAGS"]:
        feature_data[f"price_lag_{h}"] = float(baseline.get(f"price_lag_{h}") or baseline.get("price") or 0.0)
    
    for w in CONFIG["ROLL_WINDOWS_HOURS"]:
        feature_data[f"price_roll_mean_{w}h"] = float(baseline.get(f"price_roll_mean_{w}h") or baseline.get("price") or 0.0)
        feature_data[f"price_roll_std_{w}h"] = float(baseline.get(f"price_roll_std_{w}h") or 0.0)
        
    # Token features
    token = list(live_weather_token or [])
    token = token + [None] * (CONFIG["TOKEN_SIZE"][:CONFIG["TOKEN_SIZE"]])  # Pad if needed
    for i, v in enumerate(token):
        feature_data[f"w_token_{i}"] = float(v) if v is not None else None

    feature_df = spark.createDataFrame([feature_data])

    prediction = model.transform(feature_df)
    return prediction.select("prediction").toPandas()
    """
    pass

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
    
    USE_MOCK_DATA = True  # Set to False to use Hive data

    spark = create_spark_session()
    
    try:
        df = load_hive_data(spark, use_mock=USE_MOCK_DATA)
        print("Loaded rows:", df.count())
        df_features, feature_cols = prepare_features(df)
        print("Prepared features. Example columns:", feature_cols[:5])

        train, val, test, p1, p2 = time_based_split(df_features)
        print(f"Split epochs: split1={p1}, split2={p2}")
        print("Counts -> train:", train.count(), "val:", val.count(), "test:", test.count())

        evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")


        # Random Forest Pipeline 
        rf_pipeline, rf_estimator = build_pipeline(feature_cols, model_type="RF")
        rf_param_grid = ParamGridBuilder() \
            .addGrid(rf_estimator.numTrees, CONFIG["RF"]["numTrees"]) \
            .addGrid(rf_estimator.maxDepth, CONFIG["RF"]["maxDepth"]) \
            .build()
        
        rf_tvs_model = time_aware_train_val(rf_pipeline, rf_estimator, train, val, rf_param_grid, evaluator, parallelism=2)
        rf_metrics = evaluate_model(rf_tvs_model.bestModel, test, evaluator)

        print("Random Forest Test Metrics:", rf_metrics)

        # GBT Pipeline
        gbt_pipeline, gbt_estimator = build_pipeline(feature_cols, model_type="GBT")
        gbt_param_grid = ParamGridBuilder() \
            .addGrid(gbt_estimator.maxIter, CONFIG["GBT"]["maxIter"]) \
            .addGrid(gbt_estimator.maxDepth, CONFIG["GBT"]["maxDepth"]) \
            .build()
        
        gbt_tvs_model = time_aware_train_val(gbt_pipeline, gbt_estimator, train, val, gbt_param_grid, evaluator, parallelism=2)
        gbt_metrics = evaluate_model(gbt_tvs_model.bestModel, test, evaluator)
        print("GBT Test Metrics:", gbt_metrics)

        # Pick best model by RMSE
        best_model = None
        best_metrics = None
        best_type = None

        if rf_metrics["RMSE"] <= gbt_metrics["RMSE"]:
            best_model = rf_tvs_model.bestModel
            best_metrics = rf_metrics
            best_type = "RF"
        else:
            best_model = gbt_tvs_model.bestModel
            best_metrics = gbt_metrics
            best_type = "GBT"

        print(f"Best Selceted model type: {best_type} with RMSE={best_metrics['RMSE']:.4f}")

        # Save model and metadata
        metadata = {
            "created_at": now_tag(),
            "model_type": best_type,
            "test_metrics": best_metrics,
            "config": CONFIG
        }

        model_path = save_model_and_metadata(best_model, metadata, CONFIG["MODEL_OUTPUT_PATH"], CONFIG["MODEL_NAME"])

        # Demo scoring
        # Demo scoring
        sample_zone = CONFIG["SAMPLE_ZONE"]
        if sample_zone is None:
            sample_zone = df.select("zone").orderBy(F.col("ts").desc()).limit(1).collect()[0]["zone"]

        print("Demo scoring for zone:", sample_zone)

        # --- FIX STARTS HERE ---
        # 1. Choose the correct table name based on Mock vs Real data
        if USE_MOCK_DATA:
            query_table = CONFIG["HIVE_TABLE"]  # Query 'sample_table'
        else:
            query_table = f"{CONFIG['HIVE_DB']}.{CONFIG['HIVE_TABLE']}" # Query 'default.sample_table'

        latest_row = spark.sql(f"SELECT * FROM {query_table} WHERE zone = '{sample_zone}' ORDER BY ts DESC LIMIT 1").collect()
        
        if latest_row:
            live_token = latest_row[0]["token"]
            
            # 2. Use 'score_live_with_object' instead of 'score_live_weather'
            # (Because score_live_weather is empty/commented out in your file)
            pred_df = score_live_with_object(spark, best_model, live_token, zone=sample_zone, use_mock=USE_MOCK_DATA)
            
            print("Live weather token prediction:\n", pred_df)
        else:
            print("No data found for demo scoring.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()