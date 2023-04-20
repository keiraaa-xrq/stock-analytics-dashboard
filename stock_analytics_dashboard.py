from dash import Dash
from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.dags.src.utils import get_bigquery_key
from airflow.dags.src.bigquery import setup_client
from airflow.dags.src.aggregate_sentiments import aggregate_sentiments

import warnings
warnings.filterwarnings("ignore")

##### Connect to data source #####
    
bigquery_key = get_bigquery_key()
client = setup_client(f'./key/{bigquery_key}')

def retrieve_company_profile(ticker):
    company_query = f"""
    SELECT * FROM Data.Companies
    WHERE `ticker` = "{ticker}" AND `updated_time` = (
    SELECT MAX(`updated_time`)
    FROM Data.Companies
    WHERE `ticker` = "{ticker}"
    )
    """

    company_data = client.query(company_query).to_dataframe()
    company_data["updated_time"] = pd.to_datetime(company_data["updated_time"])
    company_data["updated_time"] = company_data["updated_time"].apply(lambda x: x-timedelta(hours=4))  # Converting from UTC to GMT-4
    company_data["updated_time"] = company_data["updated_time"].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
    return company_data

def retrieve_reddit_posts(ticker, limit):
    reddit_query = f"""
    SELECT * FROM Data.Reddit
    WHERE `stock_ticker` = "{ticker}"
    ORDER BY `created_time` DESC
    LIMIT {limit}
    """

    reddit_data = client.query(reddit_query).to_dataframe()
    reddit_data["created_time"] = pd.to_datetime(reddit_data["created_time"])
    reddit_data["created_time"] = reddit_data["created_time"].apply(lambda x: x-timedelta(hours=4))  # Converting from UTC to GMT-4
    reddit_data["created_time"] = reddit_data["created_time"].apply(lambda x: datetime.strftime(x, "%Y-%m-%d"))
    reddit_data = reddit_data[["stock_ticker","title","subreddit","url","upvotes","created_time"]]   # store the relevant columns only
    return reddit_data

def retrieve_stock_prices(stock_ticker, days, window):
    limit = window + days * 78 #for SMA
    stock_query = f"""
    SELECT * FROM `Yahoo.{stock_ticker}` 
    ORDER BY `Datetime` DESC
    LIMIT {limit}
    """
    
    stock_data = client.query(stock_query).to_dataframe()
    stock_data["Datetime"] = pd.to_datetime(stock_data["Datetime"])
    stock_data["Datetime"] = stock_data["Datetime"].apply(lambda x: x-timedelta(hours=4))   # Converting from UTC to GMT-4

    # stock_data.sort_values(by="Datetime", ascending=True, inplace=True)  # need to arrange in ascending order to compute SMA
    sma = stock_data["Close"].iloc[::-1].rolling(window=window).mean().tolist()  # compute SMA, "window = 20" refers to 20-period MA
    stock_data["SMA"] = sma[::-1] # reverse sma order
    return stock_data

def retrieve_sentiments(period):
    twitter_query = f"""
    SELECT tweet_id, time_pulled, sentiment 
    FROM Data.Twitter
    WHERE time_pulled IN
    (SELECT DISTINCT time_pulled
    FROM Data.Twitter
    ORDER BY time_pulled DESC 
    LIMIT {period})
    """

    twitter_data = client.query(twitter_query).to_dataframe()
    # twitter_data = twitter_data[["tweet_id", "time_pulled", "sentiment"]]
    twitter_data["time_pulled"] = pd.to_datetime(twitter_data["time_pulled"])
    twitter_data["time_pulled"] = twitter_data["time_pulled"].apply(lambda x: x-timedelta(hours=4))   # Converting from UTC to GMT-4
    twitter_data["time_pulled"] = twitter_data["time_pulled"].dt.strftime('%Y-%m-%d %H')  # "Round off" each data point to the nearest hour
    twitter_data["time_pulled"] = pd.to_datetime(twitter_data["time_pulled"])

    sentiments_df = aggregate_sentiments(twitter_data)
    """
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
    """
    return sentiments_df


##### Additional Helper Functions (Mainly for Reddit Feed) #####

def generate_markdown_string(i, title, subreddit, url, upvotes, created_time):
    return "{}. [{}]({}) \n | **r/{}** | Upvotes: {} | _{}_ \n".format(i, title, url, subreddit, int(upvotes), created_time)

def top_n_reddit_posts(df, n, ticker_name):
    markdown = "### Latest Reddit Posts (${}) 📈 \n --- \n".format(ticker_name)   # Header
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

def generate_company_profile(stock_ticker):  # remove `ticker`, `name`, `updated_time`
    """
    df_filtered = df[df["ticker"] == stock_ticker]
    df_filtered.sort_values(by="updated_time", ascending=False, inplace=True)
    df_filtered_latest = df_filtered[:1].to_dict("records")[0]
    """
    df = retrieve_company_profile(stock_ticker)
    df_filtered_latest = df.to_dict("records")[0]

    # Company Metrics
    market_cap = "$"+str(round(df_filtered_latest["market_cap"]/1000000000, 2))+"B" 
    beta = str(round(df_filtered_latest["beta"], 2))
    dividend_yield = str(round(df_filtered_latest["dividend_yield"]*100, 5)) + "%"
    peg_ratio = str(round(df_filtered_latest["peg_ratio"], 2))
    trailing_pe = str(round(df_filtered_latest["trailing_pe"], 2))
    trailing_eps = str(round(df_filtered_latest["trailing_eps"], 2))
    forward_pe = str(round(df_filtered_latest["forward_pe"], 2))
    forward_eps = str(round(df_filtered_latest["forward_eps"], 2))
    earnings_quarterly_growth = str(round(df_filtered_latest["earnings_quarterly_growth"]*100, 2)) + "%"
    earnings_annual_growth = str(round(df_filtered_latest["earnings_annual_growth"]*100, 2)) + "%"

    row1 = html.Tr([
                html.Td("Market Capitilization", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(market_cap), 
                html.Td("Beta", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(beta)
            ])
    row2 = html.Tr([
                html.Td("Dividend Yield", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(dividend_yield), 
                html.Td("PEG Ratio", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(peg_ratio)
            ])
    row3 = html.Tr([
                html.Td("Trailing P/E", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(trailing_pe), 
                html.Td("Trailing EPS", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(trailing_eps)
            ])
    row4 = html.Tr([
                html.Td("Forward P/E", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(forward_pe), 
                html.Td("Forward EPS", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(forward_eps)
            ])
    row5 = html.Tr([
                html.Td("Earnings Growth (Quarterly)", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(earnings_quarterly_growth), 
                html.Td("Earnings Growth (Annual)", style={"background-color": "#E7E7E8", "font-weight":"600"}), html.Td(earnings_annual_growth)
            ])

    table_body = [html.Tbody([row1, row2, row3, row4, row5])]
    table = dbc.Table(table_body, bordered=True)

    return dbc.Card(
                dbc.CardBody([
                        html.H4("{} (${}) 💼".format(df_filtered_latest["name"], stock_ticker), className="card-title"),
                        html.H6("Company Profile", className="card-subtitle"),
                        html.Br(),
                        html.Div(children=table),
                        html.Div(children="(last updated on {})".format(df_filtered_latest["updated_time"]), style={"font-style":"italic"}),
                        html.Br(),
                        dbc.CardLink("More Information...", href="https://finance.yahoo.com/quote/{}?p={}".format(stock_ticker, stock_ticker)),
                    ]),
                ),

def generate_twitter_widget(twitter_id):
    return html.Iframe(
                srcDoc='''
                    <a class="twitter-timeline" data-theme="dark" href="https://twitter.com/{}">
                        Loading @{}...
                    </a> 
                    <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>
                '''.format(twitter_id, twitter_id),
                style={"width": "100%", "height":"350px"},
            )

def generate_reddit_feed(stock_ticker, n=4):  # set n to 4 as default (due to sizing/aesthetics)
    """
    df_filtered = df[df["stock_ticker"] == stock_ticker]
    df_filtered.sort_values(by="created_time", ascending=False, inplace=True)
    """
    df_filtered = retrieve_reddit_posts(stock_ticker, n)

    length = min(n, df_filtered.shape[0])  # in case there is less than n number of related posts
    reddit_markdown = top_n_reddit_posts(df_filtered, length, stock_ticker)  

    return dbc.Card(
        dbc.CardBody([
            dcc.Markdown(children=reddit_markdown)
        ],
        style={"height":"375px"}
        ),
        color="light",
    )

def generate_sentiment_chart(normalize=False, period=12, show_neutral=True):
    df = retrieve_sentiments(period)
    df_filtered = df.sort_values(by="datetime", ascending=False)
    # df_filtered = df.iloc[:period]  # set to 8 as default
    
    # Data normalization
    
    if normalize:
        df_filtered["num_all"] = df_filtered["num_neg"] + df_filtered["num_neu"] + df_filtered["num_pos"]
        df_filtered["num_neg"] = df_filtered["num_neg"] / df_filtered["num_all"]
        df_filtered["num_neu"] = df_filtered["num_neu"] / df_filtered["num_all"]
        df_filtered["num_pos"] = df_filtered["num_pos"] / df_filtered["num_all"]
    # df_filtered["nps"] = df_filtered["num_pos"] - df_filtered["num_neg"]
    
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

    sentiment_chart = make_subplots(specs=[[{"secondary_y": True}]])

    # Divergent Stacked Bar Chart 
    # sentiment_chart = go.Figure()
    sentiment_type = ["num_neg", "num_neu", "num_pos"]
    for col in sentiment_type:
        sentiment_chart.add_trace(go.Bar(  
            x = df_filtered["datetime"],
            y = df_filtered[col],
            name = legend_dict[col],
            orientation = "v",
            marker_color = color_dict[col],   
        ),
        secondary_y=False,
        )
    
    sentiment_chart.update_layout(
        barmode = "relative",
        title = "General Market Sentiment (Hourly)",
        legend = dict(yanchor="top",y=0.99, xanchor="left", x=0.01)
    )

    # Add 'NPS' Line Chart
    sentiment_chart.add_trace(
        go.Scatter(
            x = df_filtered["datetime"],
            y = df_filtered["sentiment_score"],
            line = dict(color='#3C3C3D', width=1),
            name = "Sentiment Score",
        ),
        secondary_y=True
        )
    
    sentiment_chart.update_yaxes(range=[-1, 1], secondary_y=True)

    return sentiment_chart

def generate_price_chart(stock_ticker, days=3, window=20):

    df = retrieve_stock_prices(stock_ticker, days, window)
    df_filtered_recent = df.iloc[:days*78]  # each day has 78 data points (5-min intervals), show 3 days worth by default
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

# Company Profile
company_profile = generate_company_profile(default_query)

# Twitter Widget  
twitter_widget = generate_twitter_widget(default_twitter_id)

# Reddit Feed/Card
reddit_feed = generate_reddit_feed(default_query)

# Sentiment Chart 
sentiment_chart = generate_sentiment_chart()

# Price Chart
price_chart = generate_price_chart(default_query)


##### Miscellaneous #####

description = '''
### Stock Analytics Dashboard 
---
Our dashboard is a one-stop platform for in-depth insights into the performance \
of various stocks. Our dashboard provides real-time information through features \
such as: 
 - Historical Stock Price Chart
 - Market Sentiment Analysis
 - Embedded Twitter Feed
 - Related Reddit Posts

*Note: All time points are in GMT-4.*
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
            width={"size":4},
            ),
        dbc.Col(
            html.Div(
                id="company-profile",
                children=company_profile,
                ),
            width={"size":7},
            ),
        ],
        align="center",
        justify="center",
        ),
    dbc.Row([
        dbc.Col(
            html.Div(
                id="reddit-feed",
                children=reddit_feed,
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
    html.Hr(),
    dbc.Row(
        html.H4(
            children="Market-Level Information",
            style={'text-align':'center'}
            ),
        ),
    dbc.Row([
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
            dcc.Graph(
                id="sentiment-bar-chart",
                figure=sentiment_chart,
                ),
            width={"size":7},
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
    Output(component_id="company-profile", component_property="children"),
    Output(component_id="loading-output-1", component_property="children")],
    [Input(component_id="stock-ticker", component_property="value")],
    )
def update_charts(new_ticker):
    new_price_chart = generate_price_chart(new_ticker)
    new_reddit_feed = generate_reddit_feed(new_ticker)
    new_company_profile = generate_company_profile(new_ticker)
    return [new_price_chart, new_reddit_feed, new_company_profile, None]

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