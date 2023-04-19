from typing import *
import pandas as pd
import yfinance as yf
from static import TICKER_LIST


def get_stock_info(ticker: str) -> Dict[str, Any]:
    """
    Return stock related information from yahoo finance api.
    """
    fields = [
        'longName', 
        'marketCap', 
        'sharesOutstanding', 
        'beta', 
        'earningsQuarterlyGrowth', 
        'earningsGrowth', 
        'dividendYield', 
        'trailingPE', 
        'forwardPE', 
        'trailingEps', 
        'forwardEps', 
        'pegRatio'
    ]
    results = {'ticker': ticker}
    try:
        stock = yf.Ticker(ticker)
        stock_info = stock.info
        for k in fields:
            results[k] = stock_info[k]
        return results
    except Exception as e:
        print(e)
        return {}


def get_stocks_info(tickers: List[str] = TICKER_LIST) -> List[Dict]:
    """
    Get info for all the required tickers.
    """
    results = []
    for ticker in tickers:
        results.append(get_stock_info(ticker))
    return results