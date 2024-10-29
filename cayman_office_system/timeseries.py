from shining_pebbles import get_today, get_date_range
from .birdeye_connector import get_price_by_ticker
from .dataset_constants import *
from .dataset_loader import *
import pandas as pd

class Timeseries:
    def __init__(self, trades, date=None):
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        self.raw = self.open_raw_balance()
        self.initial = self.get_initial_data_of_month()
        self.frame = self.get_frame()
        self.trades = trades
        self.raw_cashflow = self.get_raw_cashflow()
        self.valuation = self.get_timeseries_valuation()
        self.df = self.get_timeseries()

    def open_raw_balance(self):
        df = open_balance_of_month(month=self.month, file_folder=file_folder['balance'])
        self.raw = df
        return df

    def get_initial_data_of_month(self):
        df = self.raw
        initial_data = get_data_balance(df)
        self.initial = initial_data
        self.initial_date = initial_data['date']
        self.initial_balance = initial_data['available_balance']
        return initial_data

    def get_frame(self):
        initial_date = self.initial_date
        initial_balance_usd = self.initial_balance
        frame_data = {'date': initial_date, 'initial_balance: usd': initial_balance_usd}
        frame = pd.DataFrame(data=frame_data, index=[0])
        frame = frame.set_index('date')
        all_dates = get_date_range(frame.index[0], get_today())
        frame = frame.reindex(all_dates)
        frame['usdkrw'] = frame.index.map(get_usdkrw_of_date)
        self.frame = frame
        return frame

    def get_raw_cashflow(self):
        df = self.trades.df[['date', 'cashflow']]
        df = df.groupby('date').sum()
        self.raw_cashflow = df
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
        frame = self.frame
        raw_cashflow = self.raw_cashflow.rename(columns={'cashflow': 'cashflow: krw'})
        df = frame.merge(raw_cashflow, how='left', left_index=True, right_index=True)
        df['cashflow: krw'] = df['cashflow: krw'].fillna(0)
        df['cashflow: usd'] = df['cashflow: krw'] / df['usdkrw']
        for i in range(len(df)):
            if i == 0:
                df.loc[df.index[i], 'initial_balance: krw'] = df.loc[df.index[i], 'initial_balance: usd'] * df.loc[df.index[i], 'usdkrw']
                df.loc[df.index[i], 'cash: krw'] = df.loc[df.index[i], 'initial_balance: krw'] + df.loc[df.index[i], 'cashflow: krw']
                df.loc[df.index[i], 'cash: usd'] = df.loc[df.index[i], 'initial_balance: usd'] + df.loc[df.index[i], 'cashflow: usd']
            else:
                df.loc[df.index[i], 'initial_balance: usd'] = df.loc[df.index[i - 1], 'cash: usd']
                df.loc[df.index[i], 'initial_balance: krw'] = df.loc[df.index[i], 'initial_balance: usd'] * df.loc[df.index[i], 'usdkrw']
                df.loc[df.index[i], 'cash: krw'] = df.loc[df.index[i], 'initial_balance: krw'] + df.loc[df.index[i], 'cashflow: krw']
                df.loc[df.index[i], 'cash: usd'] = df.loc[df.index[i], 'initial_balance: usd'] + df.loc[df.index[i], 'cashflow: usd']

        cols_for_cashflow = ['cashflow: usd', 'cashflow: krw', 'usdkrw']
        self.cashflow = df[cols_for_cashflow]
        cols_ordered = ['cash: usd', 'cash: krw', 'usdkrw']
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
        cols_ordered = ['cash: usd', 'cash: krw', 'stock: usd', 'stock: krw', 'nav: usd', 'nav: krw', 'weight_cash', 'weight_stock', 'usdkrw']
        df = df[cols_ordered]
        self.df = df
        return df

    def get_timeseries_stock(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df.copy()
        cols_for_stock = ['stock: usd', 'stock: krw', 'usdkrw']
        stock = df[cols_for_stock]
        self.stock = stock
        return stock
    
    def get_timeseries_weight(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df.copy()
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

def get_df_weight_from_df_timeseries(df):
    df['weight_cash'] = 100 * df['cash'] / df['nav']
    df['weight_stock'] = 100 * df['stock'] / df['nav']
    cols_for_weight = ['weight_cash', 'weight_stock']
    weight = df[cols_for_weight]
    return weight

def get_df_nav_from_df_timeseries(df):
    cols_for_nav = ['nav: usd', 'nav: krw', 'usdkrw']
    df = df[cols_for_nav]
    df = df[cols_for_nav].copy()
    df['return: usd'] = df['nav: usd'].pct_change().fillna(0) * 100
    df['return: krw'] = df['nav: krw'].pct_change().fillna(0) * 100
    df['cumreturn: usd'] = (df['nav: usd']/df['nav: usd'].iloc[0] - 1) * 100
    df['cumreturn: krw'] = (df['nav: krw']/df['nav: krw'].iloc[0] - 1) * 100
    cols_for_nav = ['nav: usd', 'nav: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
    nav = df[cols_for_nav]
    return nav

def get_df_price_from_df_timeseries(df):
    df['price: usd'] = (df['nav: usd'] / df['nav: usd'].iloc[0]) * 1000
    df['price: krw'] = (df['nav: krw'] / df['nav: krw'].iloc[0]) * 1000
    df['return: usd'] = df['price: usd'].pct_change().fillna(0) * 100
    df['return: krw'] = df['price: krw'].pct_change().fillna(0) * 100
    df['cumreturn: usd'] = (df['price: usd']/df['price: usd'].iloc[0] - 1) * 100
    df['cumreturn: krw'] = (df['price: krw']/df['price: krw'].iloc[0] - 1) * 100
    cols_for_price = ['price: usd', 'price: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
    price = df[cols_for_price]
    return price