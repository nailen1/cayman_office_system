from shining_pebbles import get_today, get_date_range
from .path_director import file_folder
from .dataset_constants import *
from .dataset_loader import *
from .dataset_account_controller import get_timeseries_account
from .maintanence_consts import LKEF_INCEPTION_DATE
import pandas as pd

class Timeseries:
    def __init__(self, trades, date=None):
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        # self.raw = self.open_raw_balance()
        # self.initial = self.get_initial_data_of_month()
        self.master = self.get_master()
        self.account = get_timeseries_account()
        self.trades = trades
        self.raw_cashflow = self.get_raw_cashflow()
        self.valuation = self.get_timeseries_valuation()
        self.df = self.get_timeseries()

    # def open_raw_balance(self):
    #     # df = open_balance_of_month(month=self.month, file_folder=file_folder['balance'])
    #     df = open_xls_balance(file_folder=file_folder['balance'])
    #     self.raw = df
    #     return df

    # def get_initial_data_of_month(self):
    #     df = self.raw
    #     initial_data = get_data_balance(df)
    #     self.initial = initial_data
    #     self.initial_date = initial_data['date']
    #     self.initial_balance = initial_data['available_balance']
    #     return initial_data

    def get_master(self):
        # initial_date = self.initial_date
        # initial_balance_usd = self.initial_balance
        # frame_data = {'date': initial_date, 'balance_master: usd': initial_balance_usd}
        # frame = pd.DataFrame(data=frame_data, index=[0])
        # frame = frame.set_index('date')
        cols_to_keep = ['net_flow']
        master = get_timeseries_account()[cols_to_keep].rename(columns={'net_flow': 'net_flow: usd'})
        all_dates = get_date_range(master.index[0], get_today())
        master = master.reindex(all_dates)
        # master['usdkrw'] = master.index.map(get_usdkrw_of_date)
        usdkrw = get_df_usdkrw()
        master = master.merge(usdkrw, left_index=True, right_index=True, how='left')
        master['usdkrw'] = master['usdkrw'].ffill()
        # master['credit_master: krw'] = master['credit_master: usd'] * master['usdkrw']
        # master['debit_master: krw'] = master['debit_master: usd'] * master['usdkrw']
        master['net_flow: krw'] = master['net_flow: usd'] * master['usdkrw']
        master = master.fillna(0)
        self.master = master
        return master

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
        master = self.master
        raw_cashflow = self.raw_cashflow.rename(columns={'cashflow': 'cashflow: krw'})
        df = master.merge(raw_cashflow, how='left', left_index=True, right_index=True)
        df['cashflow: krw'] = df['cashflow: krw'].fillna(0)
        df['cashflow: usd'] = df['cashflow: krw'] / df['usdkrw']
        for i in range(len(df)):
            if i == 0:
                df.loc[df.index[i], 'cash: krw'] = df.loc[df.index[i], 'net_flow: krw'] + df.loc[df.index[i], 'cashflow: krw']
                # df.loc[df.index[i], 'cash: krw'] = df.loc[df.index[i], 'credit_master: krw'] - df.loc[df.index[i], 'debit_master: krw'] + df.loc[df.index[i], 'cashflow: krw']
                # df.loc[df.index[i], 'cash: usd'] = df.loc[df.index[i], 'cash: krw'] * df.loc[df.index[i], 'usdkrw']
            else:
                df.loc[df.index[i], 'cash: krw'] = df.loc[df.index[i-1], 'cash: krw'] + df.loc[df.index[i], 'net_flow: krw'] + df.loc[df.index[i], 'cashflow: krw']
                # df.loc[df.index[i], 'cash: krw'] = df.loc[df.index[i-1], 'cash: krw'] + df.loc[df.index[i], 'credit_master: krw'] - df.loc[df.index[i], 'debit_master: krw'] + df.loc[df.index[i], 'cashflow: krw']
        df['cash: usd'] = df['cash: krw'] / df['usdkrw']
        cols_for_cashflow = ['cashflow: usd', 'cashflow: krw', 'usdkrw']
        self.cashflow = df[cols_for_cashflow]
        cols_ordered = ['cash: usd', 'cash: krw', 'net_flow: usd', 'net_flow: krw', 'cashflow: usd', 'cashflow: krw', 'usdkrw']
        # cols_ordered = ['cash: usd', 'cash: krw', 'credit_master: usd', 'credit_master: krw', 'debit_master: usd', 'debit_master: krw', 'cashflow: usd', 'cashflow: krw', 'usdkrw']
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
        nav = nav[nav.index >= LKEF_INCEPTION_DATE]
        self.nav = nav
        return nav
    
    def get_timeseries_price(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df.copy()
        price = get_df_price_from_df_timeseries(df)
        price = price[price.index >= LKEF_INCEPTION_DATE]
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
    df = df[cols_for_nav].copy()
    df['return: usd'] = df['nav: usd'].pct_change().fillna(0) * 100
    df['return: krw'] = df['nav: krw'].pct_change().fillna(0) * 100
    df['cumreturn: usd'] = (df['nav: usd']/df['nav: usd'].iloc[0] - 1) * 100
    df['cumreturn: krw'] = (df['nav: krw']/df['nav: krw'].iloc[0] - 1) * 100
    cols_for_nav = ['nav: usd', 'nav: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
    nav = df[cols_for_nav]
    return nav

def get_df_price_from_df_timeseries(df):
    # df['price: usd'] = (df['nav: usd'] / df['nav: usd'].iloc[0]) * 1000
    df['price: usd'] = (df['nav: usd'] / df['nav: usd'].loc[LKEF_INCEPTION_DATE]) * 1000
    # df['price: krw'] = (df['nav: krw'] / df['nav: krw'].iloc[0]) * 1000
    df['price: krw'] = (df['nav: krw'] / df['nav: krw'].loc[LKEF_INCEPTION_DATE]) * 1000
    df['return: usd'] = df['price: usd'].pct_change().fillna(0) * 100
    df['return: krw'] = df['price: krw'].pct_change().fillna(0) * 100
    df['cumreturn: usd'] = (df['price: usd']/df['price: usd'].loc[LKEF_INCEPTION_DATE] - 1) * 100
    df['cumreturn: krw'] = (df['price: krw']/df['price: krw'].loc[LKEF_INCEPTION_DATE] - 1) * 100
    # df.loc[df.index < LKEF_INCEPTION_DATE, 'price: usd'] = '-'
    # df.loc[df.index < LKEF_INCEPTION_DATE, 'price: krw'] = '-'
    cols_for_price = ['price: usd', 'price: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
    price = df[cols_for_price]
    return price