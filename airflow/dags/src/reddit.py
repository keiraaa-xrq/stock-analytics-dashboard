import praw
from typing import *
import datetime as dt
import pandas as pd
reddit = praw.Reddit(client_id='Rlmvoi5snEil5RLoeCymJw',
                        client_secret='oMmJOOfBMoCjtROy4WDOikhvCxF-zw',
                        user_agent='foosh')
headlines = set()

def get_reddit_post(stock_ticker):
    subreddit_list = ['stocks','wallstreetbets','stockmarket','options']    

    list_of_columns = ['stock_ticker','subreddit','id','title','url','upvotes','comments','author','created_time']
    df = pd.DataFrame(columns = list_of_columns)
    stock_ticker_lists = []
    subreddit_lists = []
    id = []
    title = []
    url = []
    upvotes = []
    comments = []
    author = []
    created_time = []

    for subreddit in subreddit_list:
        praw = reddit.subreddit(subreddit)
        posts = praw.search(stock_ticker, sort='all', time_filter='day')

        for submission in posts:
            subreddit_lists.append(subreddit)
            stock_ticker_lists.append(stock_ticker)
            url.append(submission.url)
            title.append(submission.title)
            upvotes.append(submission.score)
            comments.append(submission.num_comments)
            author.append(submission.author.name)
            id.append(submission.id)
            created_time.append(submission.created_utc)
        
    df['stock_ticker'] = stock_ticker_lists
    df['subreddit'] = subreddit_lists
    df['id'] = id
    df['title'] = title
    df['url'] = url
    df['upvotes'] = upvotes
    df['comments'] = comments
    df['author'] = author
    df['created_time'] = created_time
    
    print(len(df))
    return df

# top 30 US tech firms by market cap

def get_all_tickers():
    ticker_list = ["AAPL"]
    # "MSFT", "GOOG", "AMZN", "TSLA", "NVDA", "META", "AVGO", "ORCL", "CSCO", 
    # "CRM", "TXN", "ADBE", "NFLX", "QCOM", "AMD", "IBM", "INTU", "INTC", "AMAT",
    # "BKNG", "ADI", "ADP", "ServiceNow", "PYPL", "ABNB", "FISV", "LRCX", "UBER", "EQIX"]

    output = pd.DataFrame()
    for ticker in ticker_list:
        df = get_reddit_post(ticker)
        print(ticker)
        output = pd.concat([output,df])
    
    output_columns = list(output.columns)
    output_list = output.values.tolist()
    output_list.insert(0,output_columns)
    print(output_list)
    return output_list

def generate_reddit_df(reddit_posts):
    reddit_df = pd.DataFrame(reddit_posts[1:], columns=reddit_posts[0])
    reddit_df['stock_ticker'] = reddit_df['stock_ticker'].replace('ServiceNow','NOW')
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