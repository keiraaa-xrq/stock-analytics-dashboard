from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
import os
import pendulum
from src.big_query import setup_client, load_dataframe_to_bigquery
from src.transform import transform_stock_df, check_validity
import yfinance as yf

# top 30 US tech firms by market cap
ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
                "CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
                "BKNG", "ADI", "ADP", "NOW", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]
dataset_id="Yahoo_test.{}"
TOKEN_PATH = "token/is3107-grp18-e8944871c568.json" # use your own token
client = setup_client(TOKEN_PATH)
local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner':'airflow',
}

@dag(
    'yahoo_api_dag', 
    default_args=default_args, 
    description='Get stock price data',
    schedule_interval='*/30 * * * MON-FRI',
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['stock']
)
def yahoo_api():

    @task
    def load_stock_data(ticker, **kwargs): 
        ti = kwargs['ti']
        os.environ['NO_PROXY'] = "URL"  # for Mac OS
        print("\n#####" + ticker + "#####\n")

        data = yf.download(tickers=ticker, period='30m', interval='5m')
        stock_df = transform_stock_df(data)
        
        table_id = dataset_id.format(ticker)
        if check_validity(client, table_id, stock_df):
            load_dataframe_to_bigquery(client, table_id, stock_df)

    for ticker in ticker_list:
        load_stock_data(ticker)

yahoo_api_dag = yahoo_api()