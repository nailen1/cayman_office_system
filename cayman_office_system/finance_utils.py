import pandas as pd

def get_ticker_bbg_of_ticker(ticker):
    return f"{ticker} KS Equity".replace(' KS KS', ' KS')

map_ticker_to_ticker_bbg = get_ticker_bbg_of_ticker

def get_ticker_from_ticker_bbg(ticker_bbg):
    return ticker_bbg.replace(' Equity', '')

map_ticker_bbg_to_ticker = get_ticker_from_ticker_bbg

def format_number(number):
    return f"{number:,}"

def format_integer(number):
    return int(number) if not pd.isna(number) else number
