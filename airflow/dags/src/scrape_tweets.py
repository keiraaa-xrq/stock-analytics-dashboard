import pendulum
from typing import *
import snscrape.modules.twitter as sntwitter
import pandas as pd


TWITTER_ACCOUNTS = [
    'CNBCtech',
    'WSJmarkets',
    'YahooFinance',
    'FT',
    'IBDinvestors',
    'markets' # bloomberg markets
]

def get_tweets(twitter_accounts: List[str], start_time: pendulum.DateTime, end_time: pendulum.DateTime) -> List[Dict]:
    """
    Return all tweets posted by the user during the time window.
    """
    print("Start scraping tweets.")
    start, end = start_time.int_timestamp, end_time.int_timestamp
    tweets_list = []
    for user in twitter_accounts:
        try:
            for tweet in sntwitter.TwitterSearchScraper(f'from:{user} since:{start} until:{end} exclude:replies').get_items():
                tweets_list.append({
                    'tweet_id': tweet.id, 
                    'content': tweet.rawContent,
<<<<<<< HEAD
                    # converting to str since datetime is not JSON serialisable
                    'date': tweet.date.strftime(format='%Y-%m-%d %H:%M:%S%z'),  
=======
                    # converting to timestamp since datetime is not JSON serialisable
                    'date': tweet.date.timestamp(),  
>>>>>>> 5187d045777a5a601e81686b415bd961cb93f491
                    'url': tweet.url, 
                    'username': tweet.user.username, 
                    'retweet_count': tweet.retweetCount, 
                    'like_count': tweet.likeCount, 
                    'quote_count': tweet.quoteCount,
<<<<<<< HEAD
                    'time_pulled': end_time.to_datetime_string() 
=======
                    'time_pulled': end 
>>>>>>> 5187d045777a5a601e81686b415bd961cb93f491
                })
        except Exception as e:
            print(e)
    print("Completed scraping tweets.")
    return tweets_list


def get_tweets_n_min( 
    end_time: pendulum.DateTime,
    twitter_accounts: List[str] = TWITTER_ACCOUNTS, 
    n_min: int = 60
) -> List[Dict]:
    """
    Scrape tweets posted in the past n minutes from the list of twitter accounts.
    """

    start_time = end_time - pendulum.duration(minutes=n_min)
    tweets_list = get_tweets(twitter_accounts, start_time, end_time)
    return tweets_list
