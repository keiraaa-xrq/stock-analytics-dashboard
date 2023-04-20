from typing import *
import os
import pendulum
from airflow.decorators import dag, task
import pandas as pd
from src.reddit import *
from src.utils import get_bigquery_key
from src.bigquery import setup_client, load_dataframe_to_bigquery

@dag(
    description='Reddit Data Pipeline',
    schedule='@hourly',
    start_date=pendulum.datetime(2023, 4, 9, 0, 0, 0),
    catchup=False
)
def reddit_dag():

    @task
    def extract_reddit_task() -> List[Dict]:
        os.environ['NO_PROXY'] = "URL"
        reddit_posts = get_reddit_for_all_tickers()
        return reddit_posts
    
    @task 
    def load_reddit_task(reddit_posts):
        os.environ['NO_PROXY'] = "URL"
        key_file = get_bigquery_key()
        client = setup_client(f'./key/{key_file}')
        # load df to bigquery
        # TODO: replace with real table name
        table_id = f'{client.project}.Data.Reddit'
        reddit_df = generate_reddit_df(reddit_posts)
        load_dataframe_to_bigquery(client, table_id, reddit_df)

    # task dependdency
    reddit_posts = extract_reddit_task()
    load_reddit_task(reddit_posts)

reddit_dag()