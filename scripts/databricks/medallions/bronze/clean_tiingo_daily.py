"""script that runs after the tiingo_daily script that cleans the bronze
data, raises an error if there are missing or null values and then saves 
the cleaned data to the silver layer in adls"""

# -> explain docstring

# -> Make script
# -> One for historic
# -> one for daily

# -> Save to silver layer

# -> Test on data
import datetime as dt
from pyspark.sql import SparkSession
import pyspark.sql.functions as fs
import pyspark.sql as ps
# 1. Pull data
# 2. Clean data
# 3. push data

# I'm not sure if the first and second need to be seperate functions but the data cleaning can sure be a useful one.

# clean data 
# input: data frame
# output: cleaned dataframe
# Put some checks in the process
# make the process a little general
# use constants to set the data type and so which type of checks should be under taken
# generalise those checks for columns

# Okay so the first step is to download the data.
# Then clean. How else will I be able to clean it than using the compute. Fair enough
# just be sure the type of things you want to do.

# So overall data analysis
# 1. Check null values
# 2. 

# Want to clean all past data. 

# will select all past data and combine into one data frame
# do cleaning
# maybe a "Tiingo cleaning function"
# Do checks
# push all this data to silver
# Then have a simpler daily clean function that follows the daily update and pushes to silver

# Okay the basic daily cleaning function will be
# Check all positive
# check missing/null values
# -> send alert
# Parse/change format of date to something a bit more simple

# historical checks will include
# Contain all dates from start date
# don't have overlapped dates.


# Then save to silver

# okay

# CONSTANTS
FILE_FORMAT = "parquet"

CONTAINER = "bronze"

STORAGE_ACCOUNT = "stfinancialpatterns01"

TICKER = "voo"

# get today's date
today = dt.datetime.now()

# get spark session
spark = SparkSession.builder.getOrCreate()

# load code
# get historic
historic = spark.read.format("parquet", f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Tiingo/historic")
# Then read all past daily pushes
# try this
daily = spark.read.format("parquet", "abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/Tiingo_EOD/voo/*/*/*")

# otherwise try load 
df = historic.union(daily)
# combine the data 

# -> Apply daily cleaning function

#  now to check no dates are missing
# determine the date of the first day
# determine the date of the last day
# take the difference of dates
# this should be close to the number of rows present

#  check duplicates
duplicates = df.duplicated(keep='last')
df = df.filter(duplicates)

# now time to clean 

# Okay the basic daily cleaning function will be
# input: dataframe

# Check all positive
# check missing/null values
# -> send alert
# Parse/change format of date to something a bit more simple
# '2025-04-03T00:00:00+00:00'
# df.withColumn("modified",date_format(to_date(col("modified"), "MM/dd/yy"), "yyyy-MM-dd"))
def clean_func(dataframe: ps.DataFrame):
    """docstring"""
    # reformat the date
    dataframe.withColumn("date",
                         fs.date_format(
                             fs.to_date("date", "yyyy-MM-ddT00:00:00+00:00"),
                             "yyyy-MM-dd"))
    # sum up nans
    nans = dataframe.select(fs.count(fs.when(fs.isnan("number")==True, fs.col("number"))).alias("NaN_count"))
    # go through and check if there are any nans
    if nans.iloc[0] > 0:
        raise RuntimeError

    # check missing/null values
