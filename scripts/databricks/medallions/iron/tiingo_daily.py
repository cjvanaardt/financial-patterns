"""databricks script that is run daily to pull the EOD data for each ticker using Tiingo API"""
import sys
import os
import datetime as dt

# import from financial-patterns module
sys.path.append(os.path.abspath("/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"))

import src.medallions.iron.iron as i
import const 

# get today's date
today = dt.datetime.now()

# save data
for ticker in const.DAILY_TICKERS:
    i.save_tiingo_to_adls(
    ticker,
    const.TIINGO_API_TOKEN,
    const.STORAGE_FORMAT,
    const.STORAGE_ACCOUNT,
    const.STORAGE_ACCOUNT_KEY,
    const.BRONZE_CONTAINER,
    f"/Tiingo_EOD/{ticker}/{today.year}/{today.month}/{today.day}/")
