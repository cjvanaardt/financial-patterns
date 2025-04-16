"""functions which are used to pull data in their raw state into the bronze layer of azure data lake
service"""

import sys
import os
from typing import List, Dict
import datetime as dt
from pyspark.sql import SparkSession
import requests

sys.path.append(
    os.path.abspath(
        "/Workspace/Repos/christiaanvanaardt@hotmail.com/financial-patterns/"
    )
)

import const


def genereate_tiingo_eod_url(
    ticker: str,
    token: str,
    start: dt.date = None,
    end: dt.date = None,
    freq: str = None,
) -> str:
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
    if start or end or freq:
        url += f"&token={token}"
    else:
        url += f"token={token}"

    return url


def call_api(url: str, timeout: int = const.API_TIMEOUT) -> requests.Response:
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


def get_tiingo_eod(
    ticker: str,
    token: str,
    start: dt.date = None,
    end: dt.date = None,
    freq: str = None,
) -> List[Dict]:
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
        A list of dictionaries with each dictionary containing data representing a single EOD period

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


def save_tiingo_to_adls(
    ticker: str,
    token: str,
    fmt: str,
    storage_account: str,
    storage_key: str,
    container: str,
    path: str,
    start: dt.date = None,
    end: dt.date = None,
    freq: str = None,
) -> None:
    """Call Tiingo API for EOD data for stock defined by ticker and saves it to adls in format into the specified storage_account, container and path.

    Parameters
    ----------
    ticker : str
        ticker for the stock you want to get EOD data for
    token : str
        Tiingo API token
    fmt : str
        The format the data will be saved as.
    storage_account : str
        The storage account the data will be saved into.
    storage_key : str
        A storage account key to access storage_account.
    container : str
        The container the data will be saved into.
    path : str
        Path the data will be saved into.
    start : dt.date, optional
        If specified url will query historical data on and after the given date
    end : dt.date, optional
        If specified url will query historical data on and before the given date
    freq : str, optional
        specifies the frequency of the value returned (daily, weekly, monthly, annually)

    Return
    ------
    None

    Raises
    ------
    RuntimeError
        When the Tiingo API request response status code is not 200 (unsuccessful)
    TimeoutError
        When the API timeout limit is reached
    """
    # get Tiingo data
    data = get_tiingo_eod(ticker, token, start, end, freq)

    # get spark session
    spark = SparkSession.builder.getOrCreate()

    # transform data to dataframe
    df = spark.createDataFrame(data)

    # set the configuration for the storage account
    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_key
    )

    # save dataframe to adls
    df.write.format(fmt).save(
        f"abfss://{container}@{storage_account}.dfs.core.windows.net{path}"
    )
