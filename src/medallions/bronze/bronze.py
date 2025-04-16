"""functions for transfering data from the bronze to the silver layer"""

import pyspark.sql.functions as fs
import pyspark.sql as ps


def clean_tiingo_eod(df: ps.DataFrame) -> ps.DataFrame:
    """Takes a pyspark DataFrame created from the Tiingo EOD API call, reformats the date column and
    removes any duplciate rows.

    Parameters
    ----------
    df : pyspark DataFrame
        A pyspark DataFrame created from the Tiingo EOD API call.

    Return
    ------
    pyspark DataFrame
        A new pyspark DataFrame with reformatted date column and duplicte rows removed.
    """
    # create a copy
    dataframe = df.select('*')

    # reformat the dataframe date column
    dataframe = dataframe.withColumn("date", fs.substring(fs.col("date"), 0, 10))

    # drop duplicates
    dataframe = dataframe.dropDuplicates()

    return dataframe


def check_tiingo_eod_missing(df: ps.DataFrame) -> None:
    """Takes a pyspark dataframe created from a Tiingo EOD API call and raises an error if there are
    any NA or NULL values.

    Parameters
    ----------
    df : pyspark DataFrame
        A pyspark DataFrame created from the Tiingo EOD API call.

    Return
    ------
    None

    Raises
    ------
    RuntimeError
        When there are any Null or NAN values in the DataFrame.
    """
    # calculate the number of missing (null or nan) values
    missing = df.select(
        [fs.count(fs.when(fs.isnull(c) | fs.isnan(c), c)).alias(c) for c in df.columns]
    )
    num_missing = sum(missing.first())

    # raise an error if any are found
    if num_missing:
        raise RuntimeError
