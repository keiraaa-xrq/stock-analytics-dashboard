from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from src.big_query import setup_client, load_dataframe_to_bigquery
from google.cloud import bigquery
from google.oauth2 import service_account
import yfinance as yf

default_args = {
    'owner':'airflow',
}

@dag(
    'yahoo_api_dag', 
    default_args=default_args, 
    description='Get stock price data',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock'],
)
def yahoo_api():

    @task
    def fetch_stock_data(ticker, **kwargs): 
        ti = kwargs['ti']
        data = yf.download(tickers=ticker, period='30m', interval='5m')
        data = data.rename(columns={"Adj Close":"Adj_Close"})
        print(data)
        ti.xcom_push(f'stock_data_{ticker}', data)

    @task
    def update_bigquery_table(ticker, **kwargs):
        ti = kwargs['ti']
        stock_df = ti.xcom_pull(task_ids='fetch_stock_data', key=f'stock_data_{ticker}') 
        table_id=f"Yahoo.{ticker}",
        key_path = "../../token/is3107-grp18-e8944871c568.json" # use your own token
        client = setup_client(key_path)
        load_dataframe_to_bigquery(client, table_id, stock_df)


    # top 30 US tech firms by market cap
    ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
    "CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
    "BKNG", "ADI", "ADP", "NOW", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]

    for ticker in ticker_list:
        fetch_stock = fetch_stock_data(ticker)

        fetch_stock

    for ticker in ticker_list:
        update_bigquery_table = update_bigquery_table(ticker)

        update_bigquery_table

yahoo_api_dag = yahoo_api()