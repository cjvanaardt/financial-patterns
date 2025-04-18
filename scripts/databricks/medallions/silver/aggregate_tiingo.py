"""script to aggregate tiingo eod data from the silver layer and move it to the gold layer"""

import sys
import os
from pyspark.sql import SparkSession

# import from financial-patterns module
sys.path.append(
    os.path.abspath(
        "/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"
    )
)

import src.medallions.silver.silver as s
import const

# get pyspark session
spark = SparkSession.builder.getOrCreate()

# set spark configuration
spark.conf.set(
    f"fs.azure.account.key.{const.STORAGE_ACCOUNT}.dfs.core.windows.net",
    const.STORAGE_ACCOUNT_KEY,
)

for ticker in const.DAILY_TICKERS:
    # load data
    LOAD_PATH_END = f"/Tiingo_EOD/{ticker}"
    LOAD_PATH = f"abfss://{
        const.SILVER_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{LOAD_PATH_END}"

    data = spark.read.format(const.STORAGE_FORMAT).load(LOAD_PATH)

    # aggregate
    data = s.aggregate_tiingo_eod(data)

    # save data
    SAVE_PATH_END = f"/Tiingo_EOD/{ticker}"
    SAVE_PATH = f"abfss://{
        const.GOLD_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{SAVE_PATH_END}"

    data.write.format(const.STORAGE_FORMAT).mode('overwrite').save(SAVE_PATH)
