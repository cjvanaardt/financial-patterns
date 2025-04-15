"""module docstring"""
import sys
import os
import datetime as dt
import const

# import from financial-patterns module
sys.path.append(os.path.abspath(const.MODULE_PATH))

import src.medallions.iron.iron as i
import sensitive

# get today's date
today = dt.datetime.now()

# get historical data date
start_date = dt.date(const.HIST_DATE_YEAR, const.HIST_DATE_MONTH, const.HIST_DATE_DAY)

# Save data to ADLS
for ticker in const.HIST_TICKERS:
    i.save_tiingo_to_adls(
    ticker,
    sensitive.TIINGO_API_TOKEN,
    const.STORAGE_FILE,
    const.STORAGE_ACCOUNT,
    sensitive.STORAGE_ACCOUNT_KEY,
    const.CONTAINER,
    f"/Tiingo_EOD/{ticker}/historical/",
    start_date)
