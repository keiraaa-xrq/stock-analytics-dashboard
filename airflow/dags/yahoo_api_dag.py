import os
from airflow import DAG
from airflow.decorators import dag, task
import pandas as pd
from datetime import datetime
import pendulum
import yfinance as yf
from src.bigquery import setup_client, load_dataframe_to_bigquery
from src.transform_yahoo import transform_stock_df, check_validity, query_table_time
from src.static import TICKER_LIST
from src.utils import get_key_file_name

default_args = {
    'owner':'airflow',
}

@dag(
    'yahoo_api_dag', 
    default_args=default_args, 
    description='Fetch latest stock data',
    schedule_interval='*/30 9-16 * * MON-FRI',
    start_date=datetime(2023, 1, 1, tzinfo=pendulum.timezone("America/New_York")),
    catchup=False,
    tags=['stock'],
    concurrency=16,
    max_active_runs=10
)
def yahoo_api_dag():
    dataset_id = 'Yahoo.{}'
    key_file = get_key_file_name()
    client = setup_client(f'./key/{key_file}')

    @task(max_active_tis_per_dag=10)
    def extract_stock_data(ticker):
        os.environ['NO_PROXY'] = "URL"  # for Mac OS
        data = yf.download(tickers=ticker, period='30m', interval='5m')
        return data.to_json(orient='index', date_format='iso', date_unit='s')

    @task(max_active_tis_per_dag=10)
    def transform_stock_data(stock_json: str):
        os.environ['NO_PROXY'] = "URL"  # for Mac OS
        stock_df = pd.read_json(stock_json, orient='index')
        print(stock_df)

        table_id = dataset_id.format(ticker)
        
        table_time = query_table_time(client, table_id)

        stock_df = transform_stock_df(stock_df)
        stock_df = check_validity(stock_df, table_time)
        return stock_df.to_json(orient='records', date_format='iso', date_unit='s')
        
    
    @task(max_active_tis_per_dag=10)
    def load_stock_data(ticker, stock_json_transformed: str): 
        os.environ['NO_PROXY'] = "URL"  # for Mac OS
        stock_data = pd.read_json(stock_json_transformed, orient='records')

        table_id = dataset_id.format(ticker)

        if len(stock_data) > 0:
            load_dataframe_to_bigquery(client, table_id, stock_data)


    # Extract: call yahoo finance api to fetch stock data
    stock_json_list = []
    for ticker in TICKER_LIST:
        stock_json = extract_stock_data(ticker)
        stock_json_list.append(stock_json)

    # Transform & Load: process stock data & load it to remote db
    for i in range(len(TICKER_LIST)):
        ticker = TICKER_LIST[i]
        stock_json = stock_json_list[i]
        stock_json_transformed = transform_stock_data(stock_json)
        load_stock_data(ticker, stock_json_transformed)

yahoo_api_dag()

