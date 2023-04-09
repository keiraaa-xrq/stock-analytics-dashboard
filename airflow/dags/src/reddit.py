import praw
from typing import *
import datetime as dt
import pandas as pd
import pendulum

reddit = praw.Reddit(
    client_id='Rlmvoi5snEil5RLoeCymJw',
    client_secret='oMmJOOfBMoCjtROy4WDOikhvCxF-zw',
    user_agent='foosh'
)
# headlines = set()

def get_reddit_post(stock_ticker: str) -> List[Dict[str, Any]]:
    subreddit_list = ['stocks','wallstreetbets','stockmarket','options']
    reddit_list = []
    for subreddit in subreddit_list:
        try:
            praw = reddit.subreddit(subreddit)
            posts = praw.search(stock_ticker, sort='all', time_filter='day')

            for submission in posts:
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

def get_reddit_for_all_tickers() -> List[Dict[str, Any]]:
    # TODO: replace with all tickers
    ticker_list = ["AAPL"]
    # "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
    # "CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
    # "BKNG", "ADI", "ADP", "ServiceNow", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]

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
    reddit_df['stock_ticker'] = reddit_df['stock_ticker'].replace('ServiceNow','NOW')
    reddit_df['created_time'] = reddit_df['created_time'].apply(lambda x: pendulum.from_timestamp(x))
    return reddit_df


# def get_id_ticker(df):
#     df['stock_ticker'] = df['stock_ticker'].replace('ServiceNow','NOW')
#     id_ticker_dict = {}
#     # id_fields_dict = {}
    
#     for index, row in df.iterrows():
#         id = row['id']
#         ticker = row['stock_ticker']
#         # fields = [row['subreddit'], row['title'], row['url'],row['upvotes'],row['comments'],row['author'],row['created_time']]
#         if id in id_ticker_dict:
#             value = id_ticker_dict[id]
#             value.append(ticker)
#             id_ticker_dict[id] = value
#         else:
#             id_ticker_dict[id] = [ticker]
#         # id_fields_dict[id] = fields
        
#     # df = pd.DataFrame(list(id_ticker_dict.items()),columns = ['id','ticker']) 
#     # df.sort_values('id')

#     return id_ticker_dict


# def get_full_table(df):
#     id_ticker_dict = get_id_ticker(df)
#     df.drop(['stock_ticker'], axis=1)
#     df.drop_duplicates()
#     tickers = []
#     for index, row in df.iterrows():
#         tickers.append(id_ticker_dict[row['id']])
#     df['tickers'] = tickers
#     return df

# def get_table_with_duplicates(df):
#     df = df.drop(['subreddit','title','url','comments','upvotes','author','created_time'], axis=1)
#     return df