from typing import *
import os
import pendulum
from airflow.decorators import dag, task
from src.scrape_tweets import get_tweets_n_min
from src.transform_tweets import get_tweets_sentiments, generate_tweets_df
from src.utils import get_bigquery_key
from src.bigquery import setup_client, load_dataframe_to_bigquery

@dag(
    description='Twitter Data Pipeline',
    schedule='@hourly',
    start_date=pendulum.datetime(2023, 4, 8, 0, 0, 0),
    catchup=False
)
def twitter_dag():

    @task
    def extract_tweets_task(data_interval_end=None) -> List[Dict]:
        """
        Extract tweets using snscrape.
        """
        os.environ['NO_PROXY'] = "URL"
        dte = data_interval_end
        end_time = pendulum.datetime(dte.year, dte.month, dte.day, dte.hour, dte.minute)
        print('-'*10, f'Time pulled: {end_time}', '-'*10)
        tweets_list = get_tweets_n_min(end_time)
        print('-'*10, f'Number of tweets scraped: {len(tweets_list)}', '-'*10)
        return tweets_list
    
    @task
    def predict_sentiments_task(tweets_list: List[Dict]) -> List[int]:
        """
        Predict sentiments of the extracted tweets.
        """
        os.environ['NO_PROXY'] = "URL"
        if len(tweets_list) > 0:
            sentiments = get_tweets_sentiments(tweets_list)
            return sentiments
        else:
            return []

    @task
    def load_tweets_task(tweets_list: List[Dict], sentiments: List[int]):
        """
        Load tweets and sentiments to bigquery.
        """
        os.environ['NO_PROXY'] = "URL"
        if len(tweets_list) > 0:
            # generate tweets df
            tweets_df = generate_tweets_df(tweets_list, sentiments)
            # set up bigquery client
            key_file = get_bigquery_key()
            client = setup_client(f'./key/{key_file}')
            # load df to bigquery
            table_id = f'{client.project}.Data.Twitter'
            load_dataframe_to_bigquery(client, table_id, tweets_df)

        
    # task dependdency
    tweets_list = extract_tweets_task()
    sentiments = predict_sentiments_task(tweets_list)
    load_tweets_task(tweets_list, sentiments)


twitter_dag()
