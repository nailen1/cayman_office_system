from .trade_parser import *
from .trades import Trades
from .market_information import get_ks_equity_info

class Holdings:
    def __init__(self, start_date=None, end_date=None):
        self.trades = Trades(start_date=start_date, end_date=end_date)
        self.raw = self.get_raw()
        self.list = self.get_list()
        self.pl = self.get_pl()
        self.tickers = list(self.list.index)
        self.equities = get_ks_equity_info(self.tickers)

    def get_raw(self):
        trades = self.trades
        df = trades.pl
        names = df[['ticker', 'name']].set_index('ticker').drop_duplicates()
        cols_to_keep = ['ticker', 'num_shares', 'net_amount', 'evaluation', 'pl']
        df = df[cols_to_keep]
        df = df.groupby('ticker').sum()
        df['price_average'] = df['net_amount'] / df['num_shares']
        df['price_last'] = df['evaluation'] / df['num_shares']
        df['return'] = (df['evaluation'] / df['net_amount'] - 1)*100
        df = df.merge(names, how='left', left_index=True, right_index=True)
        self.raw = df
        return df
    
    def get_list(self):
        df = self.raw
        cols_to_keep = ['name', 'num_shares', 'net_amount', 'evaluation', 'pl', 'return']
        df = df[cols_to_keep]
        df = df.sort_values('evaluation', ascending=False)
        self.list = df
        self.tickers = list(df.index)
        self.names = list(df['name'])
        return df
    
    def get_pl(self):
        df = self.raw
        cols_to_keep = ['name', 'num_shares', 'net_amount', 'evaluation', 'pl', 'price_average', 'price_last', 'return']
        df = df[cols_to_keep]
        self.pl = df
        return df