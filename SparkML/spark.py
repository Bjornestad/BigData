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
    "APP_NAME" : "SparkApp"

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
