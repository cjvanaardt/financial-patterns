""" module docstring """
from typing import List, Dict
import datetime as dt
import requests



# const to be added to configure
API_TIMEOUT = 600

# added to invisible configure file
API_TOKEN = '934ed99db195406824f02e28f72dc7738d8f7f10'

def genereate_tiingo_eod_url(ticker: str, token: str, start: dt.date=None, end: dt.date=None, freq: str=None) -> str:
    """Generates url for Tiingo API to get EOD data for stock defined by ticker

    Parameters
    ----------
    ticker : str
        ticker for the stock you want to get EOD data for
    token : str
        Tiingo API token
    start : dt.date, optional
        If specified url will query historical data on and after the given date
    end : dt.date, optional
        If specified url will query historical data on and before the given date
    freq : str, optional
        specifies the frequency of the value returned (daily, weekly, monthly, annually)

    Returns
    -------
    string
        url to request Tiingo API to get EOD data for stock defined by ticker
    """
    # create base url
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?"

    # add start, end and freq to url if specified
    if start:
        url += f"&startDate={start.year}-{start.month}-{start.day}"
    if end:
        url += f"&endDate={end.year}-{end.month}-{end.day}"
    if freq:
        url += f"&resampleFreq={freq}"

    # add API token to the end
    if (start or end or freq):
        url += f"&token={token}"
    else:
        url += f"token={token}"

    return url

def call_api(url: str, timeout: int=API_TIMEOUT) -> requests.Response:
    """Executes a get request on url, returning a reponse if successful
    or throwing an error if unsuccessful or timeout limit is reached.

    Parameters
    ----------
    url : str 
        url which the request will try to get.
    timeout : int, optional
        Number of seconds the request will timeout after.

    Returns
    -------
    requests.Response
        If sucessful returns a requests.Reponse object

    Raises
    ------
    RuntimeError
        When the request response status code is not 200 (unsuccessful)
    TimeoutError
        When the timeout limit is reached
    """
    # get url
    response = requests.get(url, timeout=timeout)

    # if status code is not 200 raise RuntimeError
    if response.status_code != 200:
        raise RuntimeError(f"API Error. Status code: {response.status_code}")

    return response

def get_tiingo_eod(ticker: str, token: str, start: dt.date=None, end: dt.date=None, freq: str=None) -> List[Dict]:
    """Call Tiingo API for EOD data for stock defined by ticker and returns it in json format.

    Parameters
    ----------
    ticker : str 
        ticker for the stock you want to get EOD data for
    token : str
        Tiingo API token
    start : dt.date, optional
        If specified url will query historical data on and after the given date
    end : dt.date, optional
        If specified url will query historical data on and before the given date
    freq : str, optional
        specifies the frequency of the value returned (daily, weekly, monthly, annually)

    Returns
    -------
    List[Dict]
        A list of dictionaries with each dictionary representing a single Tiingo EOD period
    
    Raises
    ------
    RuntimeError
        When the Tiingo API request response status code is not 200 (unsuccessful)
    TimeoutError
        When the API timeout limit is reached

    """
    # create url
    url = genereate_tiingo_eod_url(ticker, token, start, end, freq)

    # call api
    response = call_api(url)

    return response.json()

# Next create this function.
# -> Import pyspark and use it's structures!! Easy
def save_to_adls(df: , path)
# generalise. Don't have to make it json. Make it general.

# make a save to adls function to desired location

# Then create function that calls Tiingo and saves to adls with params
# ticker
# What steps are required?
# 1. Call API
# - Calling API will be it's own function. Other two steps can exit by itself.
# 2. Convert JSON to dataframe
# 3. Save dataframe to correct file in adls
# What are other requirements?
# - Make ticker customisable
# - Make location in adls customisable.

def save_tiingo_eod_adls(ticker: str, file_path: str, format: str, token : str) -> None:
    """
    Saves EOD data for stock defined by ticker to file_path 
    in adls using format collected using Tiingo API 

    Parameters
    ----------
    ticker : string
        ticker of stock whose data you want to save
    file_path : string
        file path you want data to be saved inside adls
    format : string
        format data will be saved as in adls
    token : string
        Tiingo API token

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        when API call response is not 200 (successful)
    """
    # call API
    # data = 

    

data = get_tiingo_eod("voo", API_TOKEN)
print(data)
with open('../../../data/bronze/data.json', 'w') as json_file:
    json_file.write(str(data))
