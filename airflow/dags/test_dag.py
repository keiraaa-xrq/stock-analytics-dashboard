from typing import *
import pendulum
from airflow.decorators import dag, task
import snscrape.modules.twitter as sntwitter
from src.scrape_twitter_api import get_tweets
from src.scrape_tweets import get_tweets_n_min
from src.predict_sentiments import get_tweets_sentiments, generate_tweets_df
from src.utils import get_key_file_name
from src.bigquery import setup_client, load_dataframe_to_bigquery


@dag(
    description='Test Dag',
    schedule='@hourly',
    start_date=pendulum.datetime(2023, 3, 25, 0, 0),
    catchup=False
)
def test_dag():

    @task
    def task1(data_interval_end=None):
        print('-'*10, 'task 1', '-'*10)
        dte = data_interval_end.to_datetime_string()
        tweets = get_tweets()
        print(tweets)
    
    
    task1()


test_dag()
