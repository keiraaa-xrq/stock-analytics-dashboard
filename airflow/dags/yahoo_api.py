from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import yfinance as yf

default_args = {
    'owner':'airflow',
}

with DAG(
    dag_id='yahoo_api', 
    default_args=default_args, 
    description='Get stock price data',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stock'],
) as dag:
    dag.doc_md = "Workflow to extract stock price data and load to data big query"
    def fetch_stock_data(ticker, **kwargs): 
        ti = kwargs['ti']
        data = yf.download(tickers=ticker, period='30m', interval='5m')
        data = data.rename(columns={"Adj Close":"Adj_Close"})
        print(data)
        ti.xcom_push(f'stock_data_{ticker}', data)

    # def update_bigquery_table(ticker, **kwargs):
    #     ti = kwargs['ti']
    #     schema_fields=[
    #         {'name': 'Datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    #         {'name': 'Open', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'High', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'Low', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'Close', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'Adj_Close', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    #         {'name': 'Volume', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    #     ]
    #     destination_project_dataset_table=f"Yahoo.{ticker}",
    #     write_disposition='WRITE_TRUNCATE',


    # top 30 US tech firms by market cap
    ticker_list = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
    "CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
    "BKNG", "ADI", "ADP", "NOW", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]
    task_list = []
    for ticker in ticker_list:
        task_list.append(
            PythonOperator(task_id='fetch_stock_data', python_callable=fetch_stock_data,) 
        )

    [task for task in task_list]