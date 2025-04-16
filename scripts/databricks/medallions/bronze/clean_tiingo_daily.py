"""script that runs after the tiingo_daily script that extracts and cleans today's tiingo EOD bronze
data, raises an error if there are missing or null values and then saves
the cleaned data to the silver layer in adls"""

import sys
import os
import datetime as dt
from pyspark.sql import SparkSession

# import from financial-patterns module
sys.path.append(
    os.path.abspath(
        "/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"
    )
)

import src.medallions.bronze.bronze as b
import const

# get today's date
today = dt.datetime.now()

# get pyspark session
spark = SparkSession.builder.getOrCreate()

# set spark configuration
spark.conf.set(
    f"fs.azure.account.key.{const.STORAGE_ACCOUNT}.dfs.core.windows.net",
    const.STORAGE_ACCOUNT_KEY,
)

for ticker in const.DAILY_TICKERS:
    # define the load and save path
    PATH_END = f"/Tiingo_EOD/{ticker}/{today.year}/{today.month}/{today.day}/"
    LOAD_PATH = f"abfss://{
        const.BRONZE_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{PATH_END}"
    SAVE_PATH = f"abfss://{
        const.SILVER_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{PATH_END}"

    # load in the data
    dataframe = spark.read.format(const.STORAGE_FORMAT).load(LOAD_PATH)

    # check for missing data
    b.check_tiingo_eod_missing(dataframe)

    # clean the data
    cleaned_dataframe = b.clean_tiingo_eod(dataframe)

    # save to new location
    cleaned_dataframe.write.format(const.STORAGE_FORMAT).save(SAVE_PATH)
