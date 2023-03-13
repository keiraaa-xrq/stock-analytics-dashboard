import requests
import os
import json
from datetime import datetime, timedelta
import time
from typing import *
import re
import snscrape.modules.twitter as sntwitter
import pandas as pd


TWITTER_ACCOUNTS = [
    'CNBCtech',
    'MarketWatch',
    'WSJmarkets',
    'YahooFinance',
    'FT',
    'IBDinvestors',
    'markets' # bloomberg markets
]


def get_tweets(user: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Return all tweets posted by the user during the time window.
    """

    start, end = int(start.timestamp()), int(end.timestamp())
    tweets_list = []
    for tweet in sntwitter.TwitterSearchScraper(f'from:{user} since:{start} until:{end} exclude:replies').get_items():
        tweets_list.append([tweet.id, tweet.date, tweet.rawContent, tweet.url, tweet.user.username, tweet.retweetCount, tweet.likeCount, tweet.quoteCount, tweet.cashtags])
    tweets_df = pd.DataFrame(tweets_list, columns=['tweet_id', 'date', 'content', 'url', 'username', 'retweet_count', 'like_count', 'quote_count', 'cashtags'])
    return tweets_df


def get_tweets_30_min(twitter_accounts: List[str]) -> pd.DataFrame:
    """
    Scrape tweets posted in the past 30 minutes from the list of twitter accounts.
    """
    end = datetime.now()
    start = end - timedelta(minutes=30)
    tweet_dfs = []
    for ta in twitter_accounts:
        tweet_dfs.append(get_tweets(ta, start, end))
    tweets_df = pd.concat(tweet_dfs, ignore_index=True).sort_values('date', ascending=False, ignore_index=True)
    return tweets_df