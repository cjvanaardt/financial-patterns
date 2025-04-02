""""used to hold all constants"""
import datetime as dt


# default date historic data is captured from
DEFAULT_DATE = dt.date(1900, 1, 1)

# Yahoo Finance uses this value to represent a day in their history range
YAHOO_DAY_VALUE = 86400

# Yahoo Finance has set period=0 as this date
YAHOO_DEFAULT = dt.date(1970, 1, 1)
