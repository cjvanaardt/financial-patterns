""" module docstring """
import datetime as dt
import requests
import json


# const to be added to configure
API_TIMEOUT = 600

# added to invisible configure file
API_TOKEN = '934ed99db195406824f02e28f72dc7738d8f7f10'

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
    url = end_of_day_url(ticker, token, start, end, freq)

    # call api
    response = requests.get(url, timeout=API_TIMEOUT)

    if response.status_code != 200:
        print(url)
        raise RuntimeError(f"API Error. Status code: {response.status_code}")

    return response.json()

data = get_end_of_day("voo", API_TOKEN)
print(data)
with open('data.json', 'w') as json_file:
    json_file.write(str(data))



