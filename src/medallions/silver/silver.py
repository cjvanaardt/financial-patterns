"""functions which work with data in the silver layer"""
import datetime as dt
import pyspark.sql.functions as fs
import pyspark.sql as ps

def aggregate_tiingo_eod(df: ps.DataFrame) -> ps.DataFrame:
    """Takes a dataframe with tiingo eod data from the silver layer and
    returns a new dataframe with a new column "time" of the number of days each row comes
    after the date of the first row in the dataframe.
    
    Parameters
    ----------
    df : ps.Dataframe
        A pyspark dataframe with tiingo eod data from the silver layer.
    Returns
    -------
    ps.Dataframe
        A new pyspark dataframe with an additional column of the number
        of days each row comes after the oldest row.
    """
    # create copy of the dataframe
    dataframe = df.select('*')

     # get the first date in the dataframe
    first_date = dataframe.agg({"date": "min"}).collect()[0][0]

    start = dt.datetime(int(first_date[0:4]), int(first_date[5:7]), int(first_date[8:10]))

    # add new column
    dataframe = dataframe.withColumn(
        "date_diff",
        fs.datediff(
            fs.col("date"),
            fs.lit(start)
        )
    )
    return dataframe
