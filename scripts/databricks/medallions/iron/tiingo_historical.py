"""databricks script that is run to pull historical EOD ticker data from Tiingo API"""
import sys
import os
import datetime as dt

# import from financial-patterns module
sys.path.append(os.path.abspath("/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"))

import src.medallions.iron.iron as i
import const 

# get today's date
today = dt.datetime.now()

# get historical data date
start_date = dt.date(const.HIST_DATE_YEAR, const.HIST_DATE_MONTH, const.HIST_DATE_DAY)

# Save data to ADLS
for ticker in const.HIST_TICKERS:
    i.save_tiingo_to_adls(
    ticker,
    const.TIINGO_API_TOKEN,
    const.STORAGE_FORMAT,
    const.STORAGE_ACCOUNT,
    const.STORAGE_ACCOUNT_KEY,
    const.BRONZE_CONTAINER,
    f"/Tiingo_EOD/{ticker}/historical/",
    start_date)
