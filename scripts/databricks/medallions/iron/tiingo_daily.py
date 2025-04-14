import sys, os, const
import datetime as dt

# import from financial-patterns module
sys.path.append(os.path.abspath(sensitive.MODULE_PATH))

import src.medallions.iron.iron as i
import sensitive

# get today's date
today = datetime.now()

# save data 
for ticker in const.TICKERS:
    i.save_tiingo_to_adls(
    ticker,
    i.API_TOKEN,
    const.STORAGE_FILE, 
    const.STORAGE_ACCOUNT, 
    sensitive.STORAGE_ACCOUNT_KEY, 
    const.CONTAINER, 
    f"/Tiingo_EOD/{ticker}/{today.year}/{today.month}/{today.day}/")