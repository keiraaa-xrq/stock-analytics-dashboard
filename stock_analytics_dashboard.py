from dash import Dash
from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

import warnings
warnings.filterwarnings("ignore")

##### Connect to data source #####
    
credentials = service_account.Credentials.from_service_account_file('service_account_key.json')
project_id = "is3107-grp18"
client = bigquery.Client(credentials=credentials, project=project_id)

def retrieve_reddit_posts():
    reddit_query = """
    SELECT * 
    FROM Reddit.posts
    """

    reddit_data = client.query(reddit_query).to_dataframe()
    reddit_data["created_time"] = pd.to_datetime(reddit_data["created_time"])
    reddit_data["created_time"] = reddit_data["created_time"].apply(lambda x: x-timedelta(hours=4))  # Converting from UTC to GMT-4
    reddit_data["created_time"] = reddit_data["created_time"].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
    reddit_data = reddit_data[["stock_ticker","title","subreddit","url","upvotes","created_time"]]   # store the relevant columns only
    return reddit_data

def retrieve_stock_prices(stock_ticker):
    stock_query = """
    SELECT * 
    FROM Yahoo.{}
    """.format(stock_ticker)
    
    stock_data = client.query(stock_query).to_dataframe()
    stock_data["Datetime"] = pd.to_datetime(stock_data["Datetime"])
    stock_data["Datetime"] = stock_data["Datetime"].apply(lambda x: x-timedelta(hours=4))   # Converting from UTC to GMT-4

    stock_data.sort_values(by="Datetime", ascending=True, inplace=True)  # need to arrange in ascending order to compute SMA
    sma = stock_data["Close"].rolling(window=20).mean()  # compute SMA, "window = 20" refers to 20-period MA
    stock_data["SMA"] = sma
    return stock_data

def retrieve_sentiments():
    twitter_query = """
    SELECT * 
    FROM Twitter.Tweets
    """

    twitter_data = client.query(twitter_query).to_dataframe()
    twitter_data = twitter_data[["tweet_id", "date", "sentiment"]]
    twitter_data["date"] = pd.to_datetime(twitter_data["date"])
    twitter_data["date"] = twitter_data["date"].dt.strftime('%Y-%m-%d %H')  # "Round off" each data point to the nearest hour
    twitter_data["date"] = pd.to_datetime(twitter_data["date"])

    twitter_data_agg = twitter_data.groupby(["date", "sentiment"]).size().to_frame("count")  # aggregate the data by date and sentiment
    twitter_data_agg.reset_index(inplace=True)

    twitter_data_agg_pivot = twitter_data_agg.pivot(index='date', columns='sentiment', values='count')  # pivot the dataframe to the desired shape
    twitter_data_agg_pivot.reset_index(inplace=True)
    twitter_data_agg_pivot.fillna(0, inplace=True)
    
    twitter_data_agg_pivot = twitter_data_agg_pivot.rename_axis(None, axis=1).reset_index(drop=True)  
    twitter_data_agg_pivot.rename(columns={"date":"datetime", -1:"num_neg", 0:"num_neu", 1:"num_pos"}, inplace=True)
    twitter_data_agg_pivot[["num_neg","num_neu","num_pos"]] = twitter_data_agg_pivot[["num_neg","num_neu","num_pos"]].astype(int)
    twitter_data_agg_pivot.sort_values(by="datetime", ascending=False, inplace=True)

    sentiments = twitter_data_agg_pivot
    return sentiments


##### Additional Helper Functions (Mainly for Reddit Feed) #####

def generate_markdown_string(i, title, subreddit, url, upvotes, created_time):
    return "{}. [{}]({}) \n | **r/{}** | Upvotes: {} | _{}_ \n".format(i, title, url, subreddit, int(upvotes), created_time)

def top_n_reddit_posts(df, n):    
    markdown = "### Latest Reddit Posts 📈 \n --- \n"   # Header
    for i in range(n):
        post = df.iloc[i]
        title = post["title"]
        subreddit = post["subreddit"]
        url = post["url"]
        upvotes = post["upvotes"]
        created_time = post["created_time"]
        markdown_string = generate_markdown_string(i+1, title, subreddit, url, upvotes, created_time[:10])
        markdown = markdown + markdown_string
    return markdown


##### Functions for Generating Visualizations #####

def generate_twitter_widget(twitter_id):
    return html.Iframe(
                srcDoc='''
                    <a class="twitter-timeline" data-theme="dark" href="https://twitter.com/{}">
                        Loading @{}...
                    </a> 
                    <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>
                '''.format(twitter_id, twitter_id),
                style={"width": "100%", "height":"400px"},
            )

def generate_reddit_feed(df, stock_ticker, n=5):  # set n to 5 as default (due to sizing/aesthetics)
    df_filtered = df[df["stock_ticker"] == stock_ticker]
    df_filtered.sort_values(by="created_time", ascending=False, inplace=True)

    length = min(n, df_filtered.shape[0])  # in case there is less than n number of related posts
    reddit_markdown = top_n_reddit_posts(df_filtered, length)  

    return dbc.Card(
        dbc.CardBody([
            dcc.Markdown(children=reddit_markdown)
        ],
        style={"height":"400px"}
        ),
        color="light",
    )

def generate_sentiment_chart(df, normalize=False, period=8, show_neutral=True):
    df.sort_values(by="datetime", ascending=False, inplace=True)
    df_filtered = df.iloc[:period]  # set to 8 as default
    
    # Data normalization
    if normalize:
        df_filtered["num_all"] = df_filtered["num_neg"] + df_filtered["num_neu"] + df_filtered["num_pos"]
        df_filtered["num_neg"] = df_filtered["num_neg"] / df_filtered["num_all"]
        df_filtered["num_neu"] = df_filtered["num_neu"] / df_filtered["num_all"]
        df_filtered["num_pos"] = df_filtered["num_pos"] / df_filtered["num_all"]
    df_filtered["nps"] = df_filtered["num_pos"] - df_filtered["num_neg"]

    # Data transformation (for visualization purposes)
    df_filtered["num_neg"] = df_filtered["num_neg"] * -1   # for the divergent bar chart
    if not show_neutral:
        df_filtered["num_neu"] = 0   # set this value to zero if we don't want to display the neutral sentiments (can be interactively hidden too!)

    color_dict = {    # https://plotly.com/python/discrete-color/
        "num_neg": px.colors.qualitative.Alphabet[7],
        "num_neu": px.colors.qualitative.Pastel[10],
        "num_pos": px.colors.qualitative.Dark2[2]
    }

    legend_dict = {
        "num_neg": "Negative",
        "num_neu": "Neutral",
        "num_pos": "Positive",
    }

    # Divergent Stacked Bar Chart 
    sentiment_chart = go.Figure()
    sentiment_type = ["num_neg", "num_neu", "num_pos"]
    for col in sentiment_type:
        sentiment_chart.add_trace(go.Bar(  
            x = df_filtered["datetime"],
            y = df_filtered[col],
            name = legend_dict[col],
            orientation = "v",
            marker_color = color_dict[col],   
        ))
    
    sentiment_chart.update_layout(
        barmode = "relative",
        title = "General Market Sentiment (Hourly)",
        legend = dict(yanchor="top",y=0.99, xanchor="left", x=0.01)
    )

    # Add 'NPS' Line Chart
    sentiment_chart.add_trace(
        go.Scatter(
            x = df_filtered["datetime"],
            y = df_filtered["nps"],
            line = dict(color='#3C3C3D', width=1),
            name = "Net Sentiment",
            )
        )

    return sentiment_chart

def generate_price_chart(df, stock_ticker, days=3):

    df_filtered = df
    df_filtered.sort_values(by="Datetime", ascending=False, inplace=True)
    df_filtered_recent = df_filtered.iloc[:days*78]  # each day has 78 data points (5-min intervals), show 3 days worth by default
    price_chart = go.Figure(go.Candlestick(
        x = df_filtered_recent['Datetime'],
        open = df_filtered_recent['Open'],
        high = df_filtered_recent['High'],
        low = df_filtered_recent['Low'],
        close = df_filtered_recent['Close'],
        ))
    # Add SMA Line Chart
    price_chart.add_trace(
        go.Scatter(
            x = df_filtered_recent["Datetime"],
            y = df_filtered_recent["SMA"],
            line = dict(color='#9925be', width=1),
            name = "20-period SMA",
            )
        )
    price_chart.update_layout(
    	title="Price Chart of ${} (3-day)".format(stock_ticker),
    	showlegend=False,
    	)

    # hide outside trading hours and weekends
    price_chart.update_xaxes(
            rangeslider_visible=False,
            rangebreaks=[
                dict(bounds=["sat", "mon"]),  # hide weekends, eg. hide sat to before mon
                dict(bounds=[16, 9], pattern="hour"),  # hide hours outside of 9.30am-4pm (I changed from '930' to '9' to create a small gap so we know its a different day)
                # dict(values=["2019-12-25", "2020-12-24"])  # hide holidays (Christmas and New Year's, etc)
            ]
        )

    return price_chart


##### Charts & Widgets #####


# Let $AAPL and @WSJMarkets be the default selection (Think of it as initializing the dashboard)
default_query = "AAPL"
default_twitter_id = "WSJMarkets"

# Load Data
reddit_df = retrieve_reddit_posts()
sentiment_df = retrieve_sentiments()
price_df = retrieve_stock_prices(default_query)

# Twitter Widget  <-- not sure how to display tweets from different twitter accounts
twitter_widget = generate_twitter_widget(default_twitter_id)

# Reddit Feed/Card
reddit_feed = generate_reddit_feed(reddit_df, default_query)

# Sentiment Chart 
sentiment_chart = generate_sentiment_chart(sentiment_df)

# Price Chart
price_chart = generate_price_chart(price_df, default_query)


##### Miscellaneous #####

description = '''
### Stock Analytics Dashboard 
---
Our dashboard is one-stop platform for in-depth insights into the performance \
of various stocks. Our dashboard provides real-time information through features \
such as: 
 - Historical Stock Price Chart
 - Market Sentiment Analysis
 - Embedded Twitter Feed
 - Related Reddit Posts
'''

stock_dataset_id = "Yahoo"
stock_tables = client.list_tables(stock_dataset_id)
list_of_stocks = [stock_table.table_id for stock_table in stock_tables]


### App Layout ###

app = Dash(__name__)
app.config.external_stylesheets = [dbc.themes.BOOTSTRAP]
app.title = "Stock Analytics Dashboard"

app.layout = html.Div([
    dbc.Row(html.Br()),  # create some spacing at the top (ie. padding)
    dbc.Row([
        dbc.Col(
            dbc.Card(
                dbc.CardBody([
                    dcc.Markdown(children=description),
                    html.Br(),
                    html.B(children="Stock Ticker:"),
                    dcc.Dropdown(
                        id="stock-ticker",
                        options=list_of_stocks,
                        value=list_of_stocks[0],  # Assume to be $AAPL
                        ),
                    html.Br(),
                    ])
                ),
            width={"size":3},
            ),
        dbc.Col([
            dcc.Dropdown(
                id="twitter-acc",
                options=["WSJmarkets", "YahooFinance", "CNBCtech", "IBDinvestors", "FT", "markets"],
                value="WSJmarkets",  # Assume to be $AAPL
                ),
            html.Div(
                id="twitter-widget",
                children=twitter_widget,
                ),
            ],
            width={"size":4},
            ),
        dbc.Col(
            html.Div(
                id="reddit-feed",
                children=reddit_feed,
                ),
            width={"size":4},
            ),
        ],
        align="center",
        justify="center",
        ),
    dbc.Row([
        dbc.Col(
            dcc.Graph(
                id="sentiment-bar-chart",
                figure=sentiment_chart,
                ),
            width={"size":4},
            ),
        dbc.Col(
            dcc.Graph(
                id="price-chart",
                figure=price_chart,
                ),
            width={"size":7}
            ),
        ],
        align="center",
        justify="center",
        ),
    
##### Loading Element #####
    dbc.Row([
        dcc.Loading(
            id="loading-1",
            type="circle",
            children=html.Div(id="loading-output-1"),
            fullscreen=True,
            style={"background": "rgba(0,0,0,0.2)"}
        ),
        ]),
##########################    
    ])


# Stock Ticker Selection
@app.callback(
    [Output(component_id="price-chart", component_property="figure"),
    Output(component_id="reddit-feed", component_property="children"),
    Output(component_id="loading-output-1", component_property="children")],
    [Input(component_id="stock-ticker", component_property="value")],
    )
def update_charts(new_ticker):
    new_price_df = retrieve_stock_prices(new_ticker)
    new_price_chart = generate_price_chart(new_price_df, new_ticker)
    new_reddit_feed = generate_reddit_feed(reddit_df, new_ticker)
    return [new_price_chart, new_reddit_feed, None]

# Twitter Widget Selection
@app.callback(
    [Output(component_id="twitter-widget", component_property="children")],
    [Input(component_id="twitter-acc", component_property="value")],
    )
def update_twitter_widget(new_twitter_id):
    new_twitter_widget = generate_twitter_widget(new_twitter_id)
    return [new_twitter_widget]


if __name__ == "__main__":
    app.run_server(debug=True)