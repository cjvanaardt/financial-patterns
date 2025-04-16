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

# get historical data date
start_date = dt.date(constants.HIST_DATE_YEAR, constants.HIST_DATE_MONTH, constants.HIST_DATE_DAY)

# Save data to ADLS
for ticker in constants.HIST_TICKERS:
    i.save_tiingo_to_adls(
    ticker,
    constants.TIINGO_API_TOKEN,
    constants.STORAGE_FILE,
    constants.STORAGE_ACCOUNT,
    constants.STORAGE_ACCOUNT_KEY,
    constants.BRONZE_CONTAINER,
    f"/Tiingo_EOD/{ticker}/historical/",
    start_date)
