from typing import *
from airflow.decorators import dag, task
import pandas as pd
from src.reddit import get_all_tickers, generate_reddit_df
from src.utils import get_key_file_name 
from src.big_query import setup_client, load_dataframe_to_bigquery


def extract_reddit_task():
        data = get_all_tickers()
        return data
    

def load_reddit_task(reddit_posts):
        key_file = get_key_file_name()
        client = setup_client(f'../../key/{key_file}')
        # load df to bigquery
        table_id = f'{client.project}.Reddit.posts'
        reddit_df = generate_reddit_df(reddit_posts)
        load_dataframe_to_bigquery(client, table_id, reddit_df)


reddit_posts = extract_reddit_task()
# reddit_posts = []
load_reddit_task(reddit_posts)

