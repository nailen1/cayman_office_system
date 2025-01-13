from .dataset_constants import *
from .dataset_loader import *
from .timeseries import Timeseries
from .holdings import Holdings
    
class CaymanFund:
    def __init__(self, trades):
        self.fund_name = 'LIFE KOREA ENGAGEMENT FUND'
        self.fund_code = 'LKEF'
        self.trades = trades
        self.ts = Timeseries(trades=self.trades)
        self.date = self.ts.date
        self.month = self.ts.month
        self.holdings = Holdings(trades=self.trades)
        self.tickers = self.holdings.tickers
        self.equities = self.holdings.equities
        self.timeseries = self.get_timeserieses()
        self.portfolio = self.get_portfolio()
        self.stock = self.get_stock()


    def get_timeserieses(self):
        if not hasattr(self, 'ts'):
            self.ts = Timeseries(trades=self.trades)
        ts = self.ts
        df = ts.df
        dct = {}
        cash = ts.get_timeseries_cash()
        stock = ts.get_timeseries_stock()
        nav = ts.get_timeseries_nav()
        price = ts.get_timeseries_price()
        weight = ts.get_timeseries_weight()
        dct['df'] = df
        dct['cash'] = cash
        dct['stock'] = stock
        dct['nav'] = nav
        dct['price'] = price
        dct['weight'] = weight
        self.timeseries = dct
        return dct

    def get_portfolio(self):
        df = self.holdings.get_df()
        self.portfolio = df
        return df
    
    def get_stock(self):
        stock_objs = self.holdings.stock
        dct_dfs = {}
        for ticker, stock_obj in stock_objs.items():
            dct_dfs[ticker] = stock_obj.df
        self.stock = dct_dfs
        return dct_dfs
