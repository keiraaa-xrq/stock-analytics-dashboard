import pandas as pd
from google.cloud import bigquery
from src.big_query import run_sql_bigquery


def transform_stock_df(
        stock_df: pd.DataFrame
    ) -> pd.DataFrame:
    stock_df = stock_df.rename(columns={"Adj Close": "Adj_Close"})
    stock_df['Datetime'] = stock_df.index
    stock_df = stock_df.reset_index(drop=True)
    return stock_df


def check_validity(
        client: bigquery.Client,
        table_id: str,
        stock_df: pd.DataFrame
    ) -> bool:
    
    if len(stock_df) == 0:
        print("\n##### INVALID: stock data is empty #####\n")
        return False
    
    stock_time = stock_df.iloc[0].Datetime.to_pydatetime()

    query = """SELECT * FROM `is3107-grp18.{}` ORDER BY Datetime DESC LIMIT 1"""
    query_df = run_sql_bigquery(client, table_id, query)

    if len(query_df) == 0:
        print("\n##### VALID: table is empty #####\n")
        return True

    print("##### bigquery table datetime #####")
    table_time = query_df.iloc[0].Datetime
    print(table_time)

    if stock_time > table_time:
        print("\n##### VALID: stock data is new #####\n")
        return True
    
    print("\n##### INVALID: duplicate datapoints in table #####\n")
    return False
