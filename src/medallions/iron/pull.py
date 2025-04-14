"""thingy"""
import sys
import os
from datetime import datetime
import iron as i
# Get the absolute path of the current file (pull.py)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Move up two directories to reach src (iron -> medallions -> src)
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
# Add src to the Python path
sys.path.append(src_dir)
# Now you can import config
import sensitive

# constants
TICKER = "voo"
today = datetime.now()

i.save_tiingo_to_adls(TICKER,
                      i.API_TOKEN,
                      "parquet", 
                      "stfinancialpatterns01", 
                      sensitive.STORAGE_ACCOUNT_KEY, 
                      "bronze", 
                      f"/Tiingo_EOD/{TICKER}/{today.year}/{today.month}/{today.day}/")
