from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime
from src.big_query import setup_client, load_dataframe_to_bigquery
import yfinance as yf

# top 30 US tech firms by market cap
ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
                "CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
                "BKNG", "ADI", "ADP", "NOW", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]

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
    max_active_tasks=10,
    concurrency=10
)
def yahoo_api():
    @task
    def load_stock_data(ticker, **kwargs): 
        ti = kwargs['ti']
        data = yf.download(tickers=ticker, period='30m', interval='5m')
        data = data.rename(columns={"Adj Close":"Adj_Close"})
        data['Datetime'] = data.index
        stock_df = data.reset_index(drop=True)
        print("#####" + ticker + "#####")
        table_id=f"Yahoo.{ticker}"
        key_path = "token/is3107-grp18-e8944871c568.json" # use your own token
        client = setup_client(key_path)
        load_dataframe_to_bigquery(client, table_id, stock_df)

    for ticker in ticker_list:
        load_stock_data(ticker)

yahoo_api_dag = yahoo_api()