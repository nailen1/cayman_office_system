from .trade_parser import *
from .trades import Trades
from .timeseries import Timeseries
from .stock import Stock
from .finance_utils import *
from .market_information import get_ks_equity_info, get_mapping_ks_name

class Holdings:
    def __init__(self, trades=None):
        self.trades = self.settrades(trades)
        self.ts = self.set_timeseries()
        self.stock = self.get_stocks()
        self.df = self.get_df()
        self.tickers = self.get_tickers()
        self.equities = self.get_equities()

    def settrades(self, trades=None):
        if trades is None:
            trades = Trades()
        self.trades = trades
        return trades

    def set_timeseries(self):
        ts = Timeseries(trades=self.trades)
        self.ts = ts
        return ts
    
    def get_stocks(self):
        self.stock = self.trades.stock
        return self.stock

    def get_df_ref(self):
        stock_objs = self.stock
        dfs = [stock_obj.latest for stock_obj in stock_objs.values()]
        df = pd.concat(dfs, axis=0)
        self.date = df.iloc[0]['date']
        df = df.set_index('ticker').sort_values(by='valuation', ascending=False)
        cols_ordered = ['name', 'num_shares_cum', 'price_average', 'price_last', 'total_amount', 'valuation', 'pl']
        df_ref = df[cols_ordered].rename(columns={'num_shares_cum': 'num_shares'})
        self.df_ref = df_ref
        return df_ref
    
    def get_df(self):
        if not hasattr(self, 'df_ref'):
            self.get_df_ref()
        df = self.df_ref.copy()
        df = df[df['num_shares'] != 0]
        df['return'] = (df['valuation'] / df['total_amount'] -1) * 100
        df_nav = self.ts.df
        df_nav = df_nav[df_nav.index == self.date]
        df['weight'] = df['valuation'] / df_nav.iloc[0]['nav: krw'] * 100
        self.df = df
        return df
    
    def get_tickers(self):
        if not hasattr(self, 'df'):
            self.get_df()
        tickers = list(self.df.index)
        self.tickers = tickers
        return tickers

    def get_equities(self):
        if not hasattr(self, 'tickers'):
            self.get_tickers()
        tickers = self.tickers
        tickers_bbg = [get_ticker_bbg_of_ticker(ticker) for ticker in tickers]
        ks = get_ks_market_info()
        df = ks[ks.index.isin(tickers_bbg)]
        self.equities = df
        return df