from datetime import datetime, timedelta
from airflow.decorators import dag, task
from src.scrape_tweets import get_tweets_n_min
from src.predict_sentiments import get_tweets_sentiment
from src.aggregate_sentiments import aggregate_sentiments

@dag(
    description='Twitter Data Pipeline',
    schedule=timedelta(minutes=30),
    start_date=datetime(2023, 3, 19, 15),
    end_date=datetime(2023, 3, 19, 18),
    catchup=True
)
def twitter_dag():

    @task
    def get_tweets_task(data_interval_end=None):
        dte = data_interval_end
        end_time = datetime(dte.year, dte.month, dte.day, dte.hour, dte.minute)
        tweets_df = get_tweets_n_min(end_time)
        return tweets_df

    @task
    def get_sentiments_task(tweets_df):
        sentiments_df = get_tweets_sentiment(tweets_df)
        return sentiments_df

    @task
    def aggregate_sentiments_task(sentiments_df):
        sentiments_agg = aggregate_sentiments(sentiments_df)

    # task dependdency
    tweets_df = get_tweets_task()
    sentiments_df = get_sentiments_task(tweets_df)
    aggregate_sentiments_task(sentiments_df)

twitter_dag()
