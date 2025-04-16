"""module docstring"""
import sys
import os
import datetime as dt

# import from financial-patterns module
sys.path.append(os.path.abspath("/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"))

import src.medallions.iron.iron as i
import constants 

# get today's date
today = dt.datetime.now()

# save data
for ticker in constants.DAILY_TICKERS:
    i.save_tiingo_to_adls(
    ticker,
    constants.TIINGO_API_TOKEN,
    constants.STORAGE_FILE,
    constants.STORAGE_ACCOUNT,
    constants.STORAGE_ACCOUNT_KEY,
    constants.BRONZE_CONTAINER,
    f"/Tiingo_EOD/{ticker}/{today.year}/{today.month}/{today.day}/")
