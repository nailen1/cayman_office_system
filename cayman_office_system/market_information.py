from  shining_pebbles import get_today
from .dataset_loader import get_df_sector_ks
from .birdeye_connector import get_df_prices_of_ks_stock


class MarketInfo:
    def __init__(self, name='ks'):
        self.name = name
        self.prices = self.get_prices()
        self.sector = self.get_sector()
        self.info = self.get_info()
    
    def get_prices(self):
        mapping = {
            'ks': get_df_prices_of_ks_stock
        }
        prices = mapping[self.name]()
        self.prices = prices
        return prices
    
    def get_sector(self):
        mapping = {
            'ks': get_df_sector_ks
        }
        sector = mapping[self.name]()
        self.sector = sector
        return sector
    
    def get_info(self):
        info = self.sector.merge(self.prices, how='outer', left_index=True, right_index=True)
        cols_ordered = ['name_kr', 'name', 'market_index', 'sector', 'cap(/1e8)', 'price_last']
        info = info[cols_ordered]
        self.info = info
        return info


def get_ks_market_info():
    return MarketInfo(name='ks').info

def get_ticker_bbg_of_ticker(ticker):
    return f"{ticker} KS Equity".replace(' KS KS', ' KS')

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
