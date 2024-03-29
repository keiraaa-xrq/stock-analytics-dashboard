import pendulum
from typing import *
import snscrape.modules.twitter as sntwitter
import pandas as pd
from .static import TWITTER_ACCOUNTS


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
                    # converting to timestamp since datetime is not JSON serialisable
                    'date': tweet.date.timestamp(),  
                    'url': tweet.url, 
                    'username': tweet.user.username, 
                    'retweet_count': tweet.retweetCount, 
                    'like_count': tweet.likeCount, 
                    'quote_count': tweet.quoteCount,
                    'time_pulled': end 
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
