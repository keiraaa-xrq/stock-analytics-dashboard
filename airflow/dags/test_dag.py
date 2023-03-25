from typing import *
import pendulum
from airflow.decorators import dag, task
from src.scrape_tweets import get_tweets_n_min
from src.predict_sentiments import get_tweets_sentiments, generate_tweets_df
from src.utils import get_key_file_name
from src.bigquery import setup_client, load_dataframe_to_bigquery


@dag(
    description='Test Dag',
    schedule='@hourly',
    start_date=pendulum.datetime(2023, 3, 25, 0, 0),
    catchup=True
)
def test_dag():

    @task
    def task1(data_interval_end=None):
        print('-'*10, 'task 1', '-'*10)
        dte = data_interval_end.to_datetime_string()
        return dte
    
    @task
    def task2(dte):
        print('-'*10, 'task 2', '-'*10)
        print(dte)
    
    dte = task1()
    task2(dte)


test_dag()
