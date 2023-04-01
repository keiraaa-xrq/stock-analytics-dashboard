from typing import *
import pendulum
from airflow.decorators import dag, task
import pandas as pd
from src.reddit import get_all_tickers
from src.utils import get_key_file_name
from src.big_query import setup_client, load_dataframe_to_bigquery

@dag(
    description='Twitter Data Pipeline',
    schedule='@hourly',
    start_date=pendulum.datetime(2023, 4, 1, 0, 0),
    catchup=False
)
def reddit_dag():
    @task
    def extract_reddit_task() -> pd.DataFrame():
        data = get_all_tickers()
        return data
    
    @task 
    def load_reddit_task(reddit_posts :pd.DataFrame):
        key_file = get_key_file_name()
        client = setup_client(f'./key/{key_file}')
        # load df to bigquery
        table_id = f'{client.project}.Reddit.Posts'
        load_dataframe_to_bigquery(client, table_id, reddit_posts)

    # task dependdency
    reddit_posts = extract_reddit_task()
    load_reddit_task(reddit_posts)

reddit_dag()