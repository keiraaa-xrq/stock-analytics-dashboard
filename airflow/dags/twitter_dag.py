import pendulum
from airflow.decorators import dag, task
from src.scrape_tweets import get_tweets_n_min
from src.predict_sentiments import get_tweets_sentiment
from src.aggregate_sentiments import aggregate_sentiments
from src.bigquery import load_dataframe_to_bigquery

@dag(
    description='Twitter Data Pipeline',
    schedule='@hourly',
    start_date=pendulum.datetime(2023, 3, 20, 11, 0),
    end_date=pendulum.datetime(2023, 3, 20, 13, 0),
    catchup=False
)
def twitter_dag():

    @task
    def get_tweets_task(data_interval_end=None):
        dte = data_interval_end
        end_time = pendulum.datetime(dte.year, dte.month, dte.day, dte.hour, dte.minute)
        print('-'*10, end_time, '-'*10)
        tweets_df = get_tweets_n_min(end_time)
        print('-'*10, tweets_df.shape, '-'*10)
        try:
            load_dataframe_to_bigquery(tweets_df, 'Twitter.Tweets')
        except Exception as e:
            print(e)
        
    # task dependdency
    get_tweets_task()

twitter_dag()
