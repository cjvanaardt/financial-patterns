""" module docstring """
import datetime as dt
import requests


# const to be added to configure
API_TIMEOUT = 600

# added to invisible configure file
API_TOKEN = ''

def end_of_day_url(ticker: str, token: str, start: dt.date=None, end: dt.date=None, freq: str=None):
    """docstring"""
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?"
    if start:
        url += f"&startDate={start.year}-{start.month}-{start.day}"
    if end:

        url += f"&endDate={end.year}-{end.month}-{end.day}"
    if freq:
        url += f"&resampleFreq={freq}"
    if (start or end or freq):
        url += f"&token={token}"
    else:
        url += f"token={token}"

    return url

def get_end_of_day(ticker: str, token: str, start: dt.date=None, end: dt.date=None, freq: str=None):
    """docstring"""

    # create url
    url = end_of_day_url(ticker, token, start, end)

    # call api
    response = requests.get(url, timeout=API_TIMEOUT)

    if response.status_code != 200:
        print(url)
        raise RuntimeError(f"API Error. Status code: {response.status_code}")

    return response.json()

s = dt.date(1999, 8, 24)
e = dt.date(2025, 4, 1)
# print('somethingelse' + f"&startDate={s.year}-{s.month}-{s.day}")
#print(s)
#print(e)
print(get_end_of_day("voo", API_TOKEN, end = e))
