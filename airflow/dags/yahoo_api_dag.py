from airflow import DAG
from airflow.decorators import dag, task
import pandas as pd
from datetime import datetime
import os
import json
import time
import pendulum
from src.big_query import setup_client, load_dataframe_to_bigquery
from src.transform import transform_stock_df, check_validity, query_table_time
import yfinance as yf

os.environ['NO_PROXY'] = "URL"  # for Mac OS

# top 30 US tech firms by market cap
ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
"CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
"BKNG", "ADI", "ADP", "NOW", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]
dataset_id="Yahoo_test.{}" # for testing only
TOKEN_PATH = "token/is3107-grp18-e8944871c568.json" # use your own token
client = setup_client(TOKEN_PATH)
stock_tz = pendulum.timezone("America/New_York")

start_time = time.time() # for testing only

default_args = {
    'owner':'airflow',
}

@dag(
    'yahoo_api_dag', 
    default_args=default_args, 
    description='Fetch latest stock data',
    schedule_interval='*/30 9-16 * * MON-FRI',
    start_date=datetime(2023, 1, 1, tzinfo=stock_tz),
    catchup=False,
    tags=['stock'],
    concurrency=16,
    max_active_runs=10
)
def yahoo_api():

    @task(max_active_tis_per_dag=10)
    def extract_stock_data(ticker):
        data = yf.download(tickers=ticker, period='30m', interval='5m')
        return data.to_json(orient='index', date_format='iso', date_unit='s')

    @task(max_active_tis_per_dag=10)
    def transform_stock_data(stock_json: str):
        stock_df = pd.read_json(stock_json, orient='index')
        print(stock_df)

        table_id = dataset_id.format(ticker)
        table_time = query_table_time(client, table_id)

        stock_df = transform_stock_df(stock_df)
        stock_df = check_validity(stock_df, table_time)
        return stock_df.to_json(orient='records', date_format='iso', date_unit='s')
        
    
    @task(max_active_tis_per_dag=10)
    def load_stock_data(ticker, stock_json_transformed: str): 
        stock_data = pd.read_json(stock_json_transformed, orient='records')

        table_id = dataset_id.format(ticker)

        if len(stock_data) > 0:
            load_dataframe_to_bigquery(client, table_id, stock_data)

        ### for testing only

        end_time = time.time()
        elapsed_time = end_time - start_time

        print("elapsed_time=", elapsed_time)

    # Extract: call yahoo finance api to fetch stock data
    stock_json_list = []
    for ticker in ticker_list:
        stock_json = extract_stock_data(ticker)
        stock_json_list.append(stock_json)

    # Transform & Load: process stock data & load it to remote db
    for i in range(len(ticker_list)):
        ticker = ticker_list[i]
        stock_json = stock_json_list[i]
        stock_json_transformed = transform_stock_data(stock_json)
        load_stock_data(ticker, stock_json_transformed)

yahoo_api_dag = yahoo_api()

