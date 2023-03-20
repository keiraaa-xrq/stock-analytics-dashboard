from datetime import datetime, timedelta
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

def preprocess(tweet: str):
    """
    Remove url and trailing whitespace
    """
    tokens = tweet.split()
    tokens = tokens[:-1]
    tweet = ' '.join(tokens)
    return tweet


def get_tweets(user: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Return all tweets posted by the user during the time window.
    """

    start, end = int(start.timestamp()), int(end.timestamp())
    tweets_list = []
    for tweet in sntwitter.TwitterSearchScraper(f'from:{user} since:{start} until:{end} exclude:replies').get_items():
        tweets_list.append([tweet.id, tweet.date, tweet.rawContent, tweet.url, tweet.user.username, tweet.retweetCount, tweet.likeCount, tweet.quoteCount])
    tweets_df = pd.DataFrame(tweets_list, columns=['tweet_id', 'date', 'content', 'url', 'username', 'retweet_count', 'like_count', 'quote_count'])
    # simple preprocessing
    processed = tweets_df['content'].apply(preprocess).tolist()
    tweets_df['content'] = processed
    return tweets_df


def get_tweets_n_min(
    end_time: datetime, 
    twitter_accounts: List[str] = TWITTER_ACCOUNTS, 
    n_min: int = 30
) -> pd.DataFrame:
    """
    Scrape tweets posted in the past n minutes from the list of twitter accounts.
    """

    start_time = end_time - timedelta(minutes=n_min)
    tweet_dfs = []
    for ta in twitter_accounts:
        tweet_dfs.append(get_tweets(ta, start_time, end_time))
    tweets_df = pd.concat(tweet_dfs, ignore_index=True)
    tweets_df['time_pulled'] = [end_time] * tweets_df.shape[0]
    return tweets_df