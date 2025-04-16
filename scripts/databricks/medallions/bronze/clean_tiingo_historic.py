"""databricks python script extracts and cleans all past histoical and daily Tiingo EOD data,
raises an error if there are missing or null values and then saves the cleaned data to the silver 
layer in adls
"""

import sys
import os
from pyspark.sql import SparkSession

# import from financial-patterns module
sys.path.append(
    os.path.abspath(
        "/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"
    )
)

import src.medallions.bronze.bronze as b
import const

# get pyspark session
spark = SparkSession.builder.getOrCreate()

# set spark configuration
spark.conf.set(
    f"fs.azure.account.key.{const.STORAGE_ACCOUNT}.dfs.core.windows.net",
    const.STORAGE_ACCOUNT_KEY,
)


for ticker in const.HIST_TICKERS:
    # define the load and save path
    HISTORIC_PATH_END = f"/Tiingo_EOD/{ticker}/historical/"
    HISTORIC_LOAD_PATH = f"abfss://{
        const.BRONZE_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{HISTORIC_PATH_END}"

    DAILY_PATH_END = f"/Tiingo_EOD/{ticker}/*/*/*"
    DAILY_LOAD_PATH = f"abfss://{
        const.BRONZE_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{DAILY_PATH_END}"

    SAVE_PATH = f"abfss://{
        const.SILVER_CONTAINER}@{const.STORAGE_ACCOUNT}.dfs.core.windows.net{HISTORIC_PATH_END}"

    # load in the data
    historic = spark.read.format(const.STORAGE_FORMAT).load(HISTORIC_LOAD_PATH)
    daily = spark.read.format(const.STORAGE_FORMAT).load(DAILY_LOAD_PATH)

    # combine dataframes
    dataframe = daily.union(historic)

    # check for missing data
    b.check_tiingo_eod_missing(dataframe)

    # clean the data
    cleaned_dataframe = b.clean_tiingo_eod(dataframe)

    # save to new location
    cleaned_dataframe.write.format(const.STORAGE_FORMAT).save(SAVE_PATH)
