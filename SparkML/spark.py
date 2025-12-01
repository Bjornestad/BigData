import os
import json
import datetime
from typing import List

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, Imputer, StandardScaler
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

CONFIG = {
    "HIVE_DB" : "default",
    "HIVE_TABLE" : "sample_table",

    "MODEL_OUTPUT_PATH" : "/models/spark_model/",
    "MODEL_NAME" : "spark_model",

    "TOKEN_SIZE" : 8, # Change to match token size
    
    "TRAIN_FRAC" : 0.7,
    "VALID_FRAC" : 0.15,
    "TEST_FRAC" : 0.15,

    # Time-series feature settings (hours)
    "LAGS": [1, 2, 24],        # lag in hours
    "ROLL_WINDOWS_HOURS": [3, 24, 168],  # rolling windows in hours (3hr, 24hr, 7d)

    # Model hyperparams (search space)
    "RF": {"numTrees": [50, 100], "maxDepth": [6, 10]},
    "GBT": {"maxIter": [50, 100], "maxDepth": [4, 8]},

    "SPARK_MASTER" : "local[*]",
    "APP_NAME" : "SparkApp",

    # Zone to score for demo
    "SAMPLE_ZONE": None,

}

def now_tag():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")


def create_spark_session():
    spark = ( SparkSession.builder \
    .appName(CONFIG["APP_NAME"]) \
    .master(CONFIG["SPARK_MASTER"]) \
    .config("spark.sql.shuffle.partitions", 8) \
    .enableHiveSupport() \
    .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session created")
    return spark

# ---- Load Data -----

def load_hive_data(spark):
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
    df = df.withColumn("DayOfWeek", F.date_format(ts_col, "u").cast("int")) # 1 -> 7 (Mon -> Sun)
    df = df.withColumn("Month", F.month(ts_col))
    df = df.withColumn("is_weekend", (F.col("DayOfWeek").isin([6,7])).cast("int"))
    return df


def add_lag_and_rolling_features(df, zone_col="zone", epoch_col="ts_epoch", price_col="price", lags=[1,2], roll_windows=[24]):
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
    
    return df, feature_cols

#----- Train/Validate/Test by time split -----
def time_based_split(df, epoch_col="ts_epoch"):
    """
    Split DataFrame into train/validation/test using percentiles on epoch time.
    Ensures temporal split (no leakage)
    """
    fractions = [CONFIG["TRAIN_FRAC"], CONFIG["VALID_FRAC"], CONFIG["TEST_FRAC"]]
    assert sum(fractions) == 1.0, "Train/Valid/Test fractions must sum to 1.0"
    
    # compute split epochs: 70th and 85th percentiles for example (train up to p1, val p1 -> p2, test > p2)
    p1 = df.select(F.expr(f"percentile_approx({epoch_col}, {CONFIG["TRAIN_FRAC"]})").alias("p1")).collect()[0]["p1"]
    p2 = df.select(F.expr(f"percentile_approx({epoch_col}, {CONFIG["TRAIN_FRAC"] + CONFIG["VALID_FRAC"]})").alias("p2")).collect()[0]["p2"]

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
# Needs time-awareness

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
def score_live_weather(spark, model_path: str, live_weather_token: List[float], zone: str):
    """
    Score a live weather token by building required features:
    - fetch most recent row(s) for the zone to compute lags/rolling stats
    - build a single-row DataFrame with assembled features
    - load model and predict
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

# ----- MAIN!!!!! -----

def main(): 
    spark = create_spark_session()
    
    try:
        df = load_hive_data(spark)
        print("Loaded rows:", df.count())
        df_features, feature_cols = prepare_features(df)
        print("Prepared features. Example columns:", feature_cols[:10])

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
        sample_zone = CONFIG["SAMPLE_ZONE"]

        if sample_zone is None:
            sample_zone = df.select("zone").orderBy(F.col("ts").desc()).limit(1).collect()[0]["zone"]

        print("Demo scoring for zone:", sample_zone)

        latest_row = spark.sql(f"SELECT * FROM {CONFIG["HIVE_DB"]}.{CONFIG["HIVE_TABLE"]} WHERE zone = '{sample_zone}' ORDER BY ts DESC LIMIT 1").collect()
        if latest_row:
            live_token = latest_row[0]["weather_token"]
            pred_df = score_live_weather(spark, model_path, live_token, zone=sample_zone)
            print("Live weather token prediction:\n", pred_df)
        else:
            print("No data found for demo scoring.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()