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
