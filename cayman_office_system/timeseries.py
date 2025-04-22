from shining_pebbles import get_today, get_date_range
from .path_director import file_folder
from .dataset_constants import *
from .dataset_loader import *
from .dataset_account_controller import get_timeseries_master
from .maintanence_consts import LKEF_INCEPTION_DATE
import pandas as pd

class Timeseries:
    def __init__(self, trades, date=None):
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        self.master = self.get_master()
        self.trades = trades
        self.raw_tradeflow = self.get_raw_tradeflow()
        self.valuation = self.get_timeseries_valuation()
        self.df = self.get_timeseries()

    def get_master(self):
        master = get_timeseries_master()
        self.master = master
        return master

    def get_raw_tradeflow(self):
        df = self.trades.df[['date', 'tradeflow']]
        df = df.groupby('date').sum()
        self.raw_tradeflow = df
        return df

    def get_timeseries_valuation(self):
        if not hasattr(self, 'trades'):
            raise ValueError('Trades object is not defined')
        stock_objs = self.trades.stock
        dfs = [stock_obj.valuation for stock_obj in stock_objs.values()]
        valuations = pd.concat(dfs, axis=1).fillna(0)
        valuations['total_valuation'] = valuations.sum(axis=1)
        valuation = valuations[['total_valuation']]
        self.valuations = valuations
        self.valuation = valuation
        return valuation
    
    def get_timeseries_cash(self):
        # Initial data setup
        master = self.master
        tradeflow = self.raw_tradeflow
        df = master.merge(tradeflow, how='left', left_index=True, right_index=True).rename(columns={'tradeflow': 'tradeflow: krw'})
        df['tradeflow: krw'] = df['tradeflow: krw'].fillna(0)
        df['tradeflow: usd'] = df['tradeflow: krw'] / df['usdkrw']
        INITIAL_CASH_USD = 0.0
        df.loc[df.index[0], 'cash: usd'] = INITIAL_CASH_USD + df.loc[df.index[0]].filter(regex=': usd').sum()
        for i in range(1, len(df)):
            df.loc[df.index[i], 'cash: usd'] = df.loc[df.index[i-1], 'cash: usd'] + df.loc[df.index[i]].filter(regex=': usd').sum()
        df['cash: krw'] = df['cash: usd'] * df['usdkrw']
        cols_for_tradeflow = ['tradeflow: usd', 'tradeflow: krw']
        self.tradeflow = df[cols_for_tradeflow]
        cols_ordered = ['cash: usd', 'cash: krw', 'flow: usd', 'income: usd', 'tradeflow: usd', 'tradeflow: krw', 'usdkrw']
        self.cash = df[cols_ordered]
        return self.cash

    
    def get_timeseries(self):
        if not hasattr(self, 'cash'):
            self.get_timeseries_cash()
        cash = self.cash
        valuation = self.valuation
        valuation = valuation.rename(columns={'total_valuation': 'stock: krw'})
        df = cash.merge(valuation, how='left', left_index=True, right_index=True).fillna(0)
        df['stock: usd'] = df['stock: krw'] / df['usdkrw']
        df['nav: usd'] = df['cash: usd'] + df['stock: usd']
        df['nav: krw'] = df['cash: krw'] + df['stock: krw']
        df['weight_cash'] = 100 * df['cash: usd'] / df['nav: usd']
        df['weight_stock'] = 100 * df['stock: usd'] / df['nav: usd']
        cols_ordered = ['flow: usd', 'income: usd', 'cash: usd', 'cash: krw', 'stock: usd', 'stock: krw', 'nav: usd', 'nav: krw', 'weight_cash', 'weight_stock', 'usdkrw']
        df = df[cols_ordered]
  
        df['flow: krw'] = df['flow: usd'] * df['usdkrw']
        df['income: krw'] = df['income: usd'] * df['usdkrw']

        INITIAL_PRICE = 1000.0
        REFERENCE_PRICE_UNIT = 1000.0
        INITIAL_SHARES = 0

        # df.loc[df.index[0], 'delta_shares: usd'] = df.loc[df.index[0], 'flow: usd'] / INITIAL_PRICE * REFERENCE_PRICE_UNIT
        # df.loc[df.index[0], 'shares: usd'] = INITIAL_SHARES + df.loc[df.index[0], 'delta_shares: usd']
        # df.loc[df.index[0], 'price: usd'] = df.loc[df.index[0], 'nav: usd'] / df.loc[df.index[0], 'shares: usd'] * INITIAL_PRICE

        # for i in range(1, len(df)):
        #     df.loc[df.index[i], 'delta_shares: usd'] = df.loc[df.index[i], 'flow: usd'] / df.loc[df.index[i-1], 'price: usd'] * REFERENCE_PRICE_UNIT
        #     df.loc[df.index[i], 'shares: usd'] = df.loc[df.index[i-1], 'shares: usd'] + df.loc[df.index[i], 'delta_shares: usd']
        #     df.loc[df.index[i], 'price: usd'] = df.loc[df.index[i], 'nav: usd'] / df.loc[df.index[i], 'shares: usd'] * INITIAL_PRICE

        # df.loc[df.index[0], 'delta_shares: krw'] = df.loc[df.index[0], 'flow: krw'] / INITIAL_PRICE * REFERENCE_PRICE_UNIT
        # df.loc[df.index[0], 'shares: krw'] = INITIAL_SHARES + df.loc[df.index[0], 'delta_shares: krw']
        # df.loc[df.index[0], 'price: krw'] = df.loc[df.index[0], 'nav: krw'] / df.loc[df.index[0], 'shares: krw'] * INITIAL_PRICE

        # for i in range(1, len(df)):
        #     df.loc[df.index[i], 'delta_shares: krw'] = df.loc[df.index[i], 'flow: krw'] / df.loc[df.index[i-1], 'price: krw'] * REFERENCE_PRICE_UNIT
        #     df.loc[df.index[i], 'shares: krw'] = df.loc[df.index[i-1], 'shares: krw'] + df.loc[df.index[i], 'delta_shares: krw']
        #     df.loc[df.index[i], 'price: krw'] = df.loc[df.index[i], 'nav: krw'] / df.loc[df.index[i], 'shares: krw'] * INITIAL_PRICE
        
        for suffix in [': usd', ': krw']:
            df.loc[df.index[0], f'delta_shares{suffix}'] = df.loc[df.index[0], f'flow{suffix}'] / INITIAL_PRICE * REFERENCE_PRICE_UNIT
            df.loc[df.index[0], f'shares{suffix}'] = INITIAL_SHARES + df.loc[df.index[0], f'delta_shares{suffix}']
            df.loc[df.index[0], f'price{suffix}'] = df.loc[df.index[0], f'nav{suffix}'] / df.loc[df.index[0], f'shares{suffix}'] * INITIAL_PRICE

            for i in range(1, len(df)):
                df.loc[df.index[i], f'delta_shares{suffix}'] = df.loc[df.index[i], f'flow{suffix}'] / df.loc[df.index[i-1], f'price{suffix}'] * REFERENCE_PRICE_UNIT
                df.loc[df.index[i], f'shares{suffix}'] = df.loc[df.index[i-1], f'shares{suffix}'] + df.loc[df.index[i], f'delta_shares{suffix}']
                df.loc[df.index[i], f'price{suffix}'] = df.loc[df.index[i], f'nav{suffix}'] / df.loc[df.index[i], f'shares{suffix}'] * INITIAL_PRICE

        cols_ordered = ['flow: usd', 'income: usd', 'cash: usd', 'stock: usd', 'nav: usd', 'delta_shares: usd', 'shares: usd', 'price: usd', 'flow: krw', 'income: krw', 'cash: krw','stock: krw', 'nav: krw',
       'delta_shares: krw', 'shares: krw', 'price: krw', 'weight_cash', 'weight_stock', 'usdkrw']
        df = df[cols_ordered]

        self.df = df

        return df
    
    def get_timeseries_stock(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df
        cols_for_stock = ['stock: usd', 'stock: krw', 'usdkrw']
        stock = df[cols_for_stock]
        self.stock = stock
        return stock
    
    def get_timeseries_weight(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df
        cols_for_weight = ['weight_cash', 'weight_stock']
        weight = df[cols_for_weight]
        self.weight = weight
        return weight


    def get_timeseries_nav(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df.copy()
        nav = get_df_nav_from_df_timeseries(df)
        self.nav = nav
        return nav
    
    def get_timeseries_price(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df.copy()
        price = get_df_price_from_df_timeseries(df)
        self.price = price
        return price

def get_df_nav_from_df_timeseries(df):
    df['return: usd'] = df['nav: usd'].pct_change().fillna(0) * 100
    df['return: krw'] = df['nav: krw'].pct_change().fillna(0) * 100
    df['cumreturn: usd'] = (df['nav: usd']/df['nav: usd'].iloc[0] - 1) * 100
    df['cumreturn: krw'] = (df['nav: krw']/df['nav: krw'].iloc[0] - 1) * 100
    cols_for_nav = ['nav: usd', 'nav: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
    nav = df[cols_for_nav]
    return nav

def get_df_price_from_df_timeseries(df):
    df['return: usd'] = df['price: usd'].pct_change().fillna(0) * 100
    df['return: krw'] = df['price: krw'].pct_change().fillna(0) * 100
    df['cumreturn: usd'] = (df['price: usd']/df['price: usd'].iloc[0] - 1) * 100
    df['cumreturn: krw'] = (df['price: krw']/df['price: krw'].iloc[0] - 1) * 100
    # df['cumreturn: usd'] = (df['price: usd']/df['price: usd'].loc[LKEF_INCEPTION_DATE] - 1) * 100
    # df['cumreturn: krw'] = (df['price: krw']/df['price: krw'].loc[LKEF_INCEPTION_DATE] - 1) * 100
 
    cols_for_price = ['price: usd', 'price: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
    price = df[cols_for_price]
    return price