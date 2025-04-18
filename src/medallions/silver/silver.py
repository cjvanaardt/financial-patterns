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
    first_row = dataframe.sort("date", ascending=True).first()

    first_date = first_row["date"]
    # first_row.__getitem__("date")

    start = dt.datetime(int(first_date[0:4]), int(first_date[5:7]), int(first_date[8:10]))

    # create new column based on days from the earliest row

    dataframe.withColumn("date",
                         (dt.datetime(int(fs.col("date")[0:4]),
                                      int(fs.col("date")[5:7]),
                                       int(fs.col("date")[8:10])) - start).days)

    return dataframe
