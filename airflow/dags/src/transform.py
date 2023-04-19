import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
from src.big_query import run_sql_bigquery

def transform_stock_df(
        stock_df: pd.DataFrame
    ) -> pd.DataFrame:
    stock_df = stock_df.rename(columns={"Adj Close": "Adj_Close"})
    stock_df['Datetime'] = stock_df.index
    stock_df = stock_df.reset_index(drop=True)
    return stock_df

def query_table_time(
        client: bigquery.Client,
        table_id: str,
    ) -> str:
    query = """SELECT Datetime FROM `is3107-grp18.{}` ORDER BY Datetime DESC LIMIT 1"""
    time_df = run_sql_bigquery(client, table_id, query)

    if len(time_df) == 0:
        print("\n##### table is empty #####\n")
        return None
    
    print("##### return table time #####")
    table_time = time_df.iloc[0].Datetime
    print(table_time)
    return table_time

def check_validity(
        stock_df: pd.DataFrame,
        table_time: datetime.date
    ) -> pd.DataFrame:
    
    if len(stock_df) == 0:
        print("\n##### INVALID: stock data is empty #####\n")
        return pd.DataFrame()
    
    if table_time == None:
        print("\n##### VALID: table is empty #####\n")
        return stock_df

    for i in range(len(stock_df)):
        print("##### stock datetime #####")
        stock_time = stock_df.iloc[i].Datetime.astimezone(timezone.utc)
        print(stock_time)
        print("##### table datetime #####")
        print(table_time)
        
        if stock_time > table_time:
            print("##### no overlop #####\n")
            return stock_df.iloc[i:]

        print("##### current row overlop #####\n")
        
    return pd.DataFrame()

