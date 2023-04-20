from typing import *
import json
import praw
import pandas as pd
import pendulum
from .static import SUBREDDIT_LIST, TICKER_LIST
from .utils import get_reddit_key

reddit = praw.Reddit(
    client_id='Rlmvoi5snEil5RLoeCymJw',
    client_secret='oMmJOOfBMoCjtROy4WDOikhvCxF-zw',
    user_agent='foosh'
)
# headlines = set()

def setup_praw_reddit():
    key_file_name = get_reddit_key()
    with open(f'./key/{key_file_name}', 'r') as f:
        key_file = json.load(f)
    try:
        client_id = key_file['client_id']
        client_secret = key_file['client_secret']
        user_agent = key_file['user_agent']
    except Exception as e:
        print(f'Error in retrieving required fields from key file: {e}')
        raise e
    try:
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        return reddit
    except Exception as e:
        print(f'Error in setting up praw for reddit: {e}')
        raise e
    

def get_reddit_post(stock_ticker: str, subreddit_list: List[str] = SUBREDDIT_LIST) -> List[Dict[str, Any]]:
    reddit = setup_praw_reddit()
    reddit_list = []
    for subreddit in subreddit_list:
        try:
            praw = reddit.subreddit(subreddit)
            posts = praw.search(stock_ticker, sort='all', time_filter='hour')

            for submission in posts:
                if submission.url[-3:] == "jpg" or submission.url[-3:] == "png":
                    continue
                reddit_list.append({
                    'stock_ticker': stock_ticker,
                    'subreddit': subreddit,
                    'id': submission.id,
                    'title': submission.title,
                    'url': submission.url,
                    'upvotes': submission.score,
                    'num_comments': submission.num_comments,
                    'author': submission.author.name,
                    'created_time': submission.created_utc # timestamp
                })
        except Exception as e:
            print(e)
    return reddit_list

# top 30 US tech firms by market cap

def get_reddit_for_all_tickers(ticker_list: List[str] = TICKER_LIST) -> List[Dict[str, Any]]:

    reddit_all = []
    for ticker in ticker_list:
        print(f"Scraping Reddit for {ticker}.")
        reddit_list = get_reddit_post(ticker)
        print(f"Number of posts for {ticker}: {len(reddit_list)}")
        reddit_all.extend(reddit_list)
    
    print(f"Total number of posts: {len(reddit_all)}")
    return reddit_all

def generate_reddit_df(reddit_posts: List[Dict[str, Any]]) -> pd.DataFrame:
    reddit_df = pd.DataFrame(reddit_posts)
    if len(reddit_df)!=0:
        reddit_df['stock_ticker'] = reddit_df['stock_ticker'].replace('ServiceNow','NOW')
        reddit_df['created_time'] = reddit_df['created_time'].apply(lambda x: pendulum.from_timestamp(x))
    return reddit_df


if __name__ == '__main__':
    reddits = get_reddit_post('AAPL')
    print(reddits)