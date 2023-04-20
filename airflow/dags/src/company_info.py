from typing import *
import pandas as pd
import yfinance as yf
from .static import TICKER_LIST


def get_company_info(ticker: str) -> Dict[str, Any]:
    """
    Return stock related information from yahoo finance api.
    """
    fields = {
        'name': 'longName', 
        'market_cap': 'marketCap', 
        'shares_outstanding': 'sharesOutstanding', 
        'beta': 'beta', 
        'earnings_quarterly_growth': 'earningsQuarterlyGrowth', 
        'earnings_annual_growth': 'earningsGrowth', 
        'dividend_yield': 'dividendYield', 
        'trailing_pe': 'trailingPE', 
        'forward_pe': 'forwardPE', 
        'trailing_eps': 'trailingEps', 
        'forward_eps': 'forwardEps', 
        'peg_ratio': 'pegRatio'
    }
    results = {'ticker': ticker}
    stock = yf.Ticker(ticker)
    try:
        stock_info = stock.info
    except Exception as e:
        print(f'No info found for {ticker}. Error: {e}')
        return {}
    for k, v in fields.items():
        try:
            results[k] = stock_info[v]
        except Exception as e:
            print(f'Error in retrieving {v} for {ticker}: {e}')
    return results


def get_companies_info(tickers: List[str] = TICKER_LIST) -> List[Dict]:
    """
    Get info for all the required tickers.
    """
    results = []
    for ticker in tickers:
        info = get_company_info(ticker)
        if info:
            results.append(info)
    return results