from shining_pebbles import get_today
from cayman_office_system.finance_utils import get_ticker_from_ticker_bbg, get_ticker_bbg_of_ticker
from .market_information import MarketInformation
from .pseudo_database import get_df_sector_ks

def get_mapping_ks_name():
    name_sector = get_df_sector_ks()
    return name_sector['name'].to_dict()

def get_mapping_ks_sector():
    name_sector = get_df_sector_ks()
    return name_sector['sector'].to_dict()

def get_ks_market_info():
    return MarketInformation(name='ks').info

def append_market_info_to_df(df):
    df['ticker_bbg'] = df['ticker'].map(lambda x: get_ticker_bbg_of_ticker(x))
    ks = get_ks_market_info().reset_index()
    df = df.merge(ks, how='left', on='ticker_bbg')
    return df

def get_ks_equity_info(tickers):
    tickers_bbg = [get_ticker_bbg_of_ticker(ticker) for ticker in tickers]
    ks = get_ks_market_info()
    df = ks[ks.index.isin(tickers_bbg)]
    df.columns.name = get_today()
    return df

def get_list_of_ks_stocks():
    stock_info = get_ks_market_info()
    stock_info['ticker'] = stock_info.index.map(get_ticker_from_ticker_bbg)
    stock_info = stock_info.fillna('-')
    return stock_info

