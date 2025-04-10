"""thingy"""
from datetime import datetime
import iron as i

# constants
TICKER = "voo"
today = datetime.now()

i.save_tiingo_to_adls(TICKER,
                      i.API_TOKEN,
                      "parquet", 
                      "stfinancialpatterns01", 
                      "bronze", 
                      f"/Tiingo_EOD/{TICKER}{today.year}{today.month}{today.day}")
