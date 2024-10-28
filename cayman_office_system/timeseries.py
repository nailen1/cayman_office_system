from shining_pebbles import get_today, get_date_range
from .birdeye_connector import get_price_by_ticker
from .dataset_constants import *
from .dataset_loader import *
from .stock import Stock
import pandas as pd

class Timeseries:
    def __init__(self, trades, date=None):
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        self.raw = self.open_raw_balance()
        self.initial = self.get_initial_data_of_month()
        self.frame = self.get_frame()
        self.trades = trades
        # self._raw = self.trades._raw
        self.buys = self.trades.buys
        self.sells = self.trades.sells
        self.raw_cashflow = self.get_raw_cashflow()
        self.valuation = self.get_timeseries_valuation()
        self.df = self.get_timeseries()
        # self.df = self.get_approx_timeseries()
        # self.valuation = self.get_timeseries_valuation()
        # self.cashflow = self.get_timeseries_cashflow()
        # self.cash = self.get_timeseries_cash()
        # self.stock = self.get_timeserise_of_stock()
        # self.df = self.get_timeseries()

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
        # df['total_cashflow'] = df['cashflow'].cumsum()
        # cashflow = df[['total_cashflow']]
        # self.cashflows = df
        self.raw_cashflow = df
        return df

    def get_timeseries_valuation(self):
        valuations = []
        for k, df in self.trades.dfs.items():
            stock = Stock(df=df)
            valuation = stock.valuation
            valuations.append(valuation)
        valuations = pd.concat(valuations, axis=1).fillna(0)
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
        cols_ordered = ['cash: usd', 'cash: krw', 'stock: usd', 'stock: krw', 'nav: usd', 'nav: krw', 'usdkrw']
        df = df[cols_ordered]
        self.df = df
        return df

    # def get_approx_timeseries(self):
    #     frame = self.frame
    #     cashflow = self.cashflow
    #     df = frame.merge(cashflow, how='left', left_index=True, right_index=True)
    #     df['cashflow'] = df['cashflow'].fillna(0)
    #     df['initial_balance: usd'] = df['initial_balance: usd'].ffill()
    #     df['initial_balance: krw'] = df['initial_balance: usd'] * df['usdkrw']
    #     df['cashflow_cum: krw'] = df['cashflow'].cumsum()
    #     df['cash: krw'] = df['initial_balance: krw'] + df['cashflow_cum: krw']
    #     df['cash: usd'] = df['cash: krw'] / df['usdkrw']
        
    #     valuation = self.valuation
    #     df = df.merge(valuation, how='left', left_index=True, right_index=True).fillna(0)
    #     df['stock: krw'] = df['total_valuation']
    #     df['stock: usd'] = df['stock: krw'] / df['usdkrw'] 
    #     df['nav: usd'] = df['cash: usd'] + df['stock: usd']
    #     df['nav: krw'] = df['cash: krw'] + df['stock: krw']

    #     cols_to_keep = ['cash: usd', 'cash: krw', 'stock: usd', 'stock: krw', 'nav: usd', 'nav: krw', 'usdkrw']
    #     df = df[cols_to_keep]
    #     self.df = df
    #     return df

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
        df['weight_cash: usd'] = 100 * df['cash: usd'] / df['nav: usd']
        df['weight_cash: krw'] = 100 * df['cash: krw'] / df['nav: krw']
        df['weight_stock: usd'] = 100 * df['stock: usd'] / df['nav: usd']
        df['weight_stock: krw'] = 100 * df['stock: krw'] / df['nav: krw']
        cols_for_weight = ['weight_cash: usd', 'weight_cash: krw', 'weight_stock: usd', 'weight_stock: krw']
        weight = df[cols_for_weight]
        self.weight = weight
        return weight
    
    def get_timeseries_nav(self):
        if not hasattr(self, 'df'):
            self.get_timeseries()
        df = self.df
        cols_for_nav = ['nav: usd', 'nav: krw', 'usdkrw']
        df = df[cols_for_nav]
        df = df[cols_for_nav].copy()
        df['return: usd'] = df['nav: usd'].pct_change().fillna(0) * 100
        df['return: krw'] = df['nav: krw'].pct_change().fillna(0) * 100
        df['cumreturn: usd'] = (df['nav: usd']/df['nav: usd'].iloc[0] - 1) * 100
        df['cumreturn: krw'] = (df['nav: krw']/df['nav: krw'].iloc[0] - 1) * 100
        cols_for_nav = ['nav: usd', 'nav: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
        nav = df[cols_for_nav]
        self.nav = nav
        return nav
    
    def get_timeseries_price(self):
        if not hasattr(self, 'nav'):
            self.get_timeseries_nav()
        df = self.nav.copy()
        df['price: usd'] = (df['nav: usd'] / df['nav: usd'].iloc[0]) * 1000
        df['price: krw'] = (df['nav: krw'] / df['nav: krw'].iloc[0]) * 1000
        df['return: usd'] = df['price: usd'].pct_change().fillna(0) * 100
        df['return: krw'] = df['price: krw'].pct_change().fillna(0) * 100
        df['cumreturn: usd'] = (df['price: usd']/df['price: usd'].iloc[0] - 1) * 100
        df['cumreturn: krw'] = (df['price: krw']/df['price: krw'].iloc[0] - 1) * 100
        cols_for_price = ['price: usd', 'price: krw', 'return: usd', 'return: krw', 'cumreturn: usd', 'cumreturn: krw', 'usdkrw']
        price = df[cols_for_price]
        self.price = price
        return price
    
    
    #     # merged_df = self._raw
    #     # df_nums = merged_df[['date', 'ticker', 'delta_shares']]
    #     # dct = {}
    #     # for ticker in df_nums['ticker'].unique():
    #     #     df = df_nums[df_nums['ticker'] == ticker].copy()
    #     #     df = df.drop(columns='ticker')
    #     #     df = df.groupby('date').sum()
    #     #     all_dates = get_date_range(df.index[0], get_today())
    #     #     df = df.reindex(all_dates)
    #     #     df['delta_shares'] = df['delta_shares'].fillna(0)
    #     #     df[f'num_shares: {ticker}'] = df['delta_shares'].cumsum()
    #     #     df_price = get_price_by_ticker(ticker).loc[df.index.min():]
    #     #     df = df.merge(df_price, how='left', left_index=True, right_index=True)
    #     #     df['price_last'] = df['price_last'].ffill()
    #     #     df[ticker] = df[f'num_shares: {ticker}'] * df['price_last']
    #     #     dct[ticker] = df[[ticker]]
    #     # dfs = list(dct.values())
    #     # # df = pd.concat(dfs, axis=1, join='outer')
    #     # df = pd.concat(dfs, axis=1).fillna(0)
    #     # self._valuations = df
    #     # df['valuation'] = df.sum(axis=1)
    #     # df = df[['valuation']]
    #     # self.valuation = df
    #     # return df

    # # def get_timeseries_cashflow(self):
    # #     df_buys = self.buys[['date', 'net_amount']]
    # #     df_sells = self.sells[['date', 'realization']]
    # #     df = pd.concat([df_buys, df_sells]).sort_values('date').fillna(0).groupby('date').sum()
    # #     df['cashflow'] = -df['net_amount'] + df['realization']
    # #     all_dates = get_date_range(df.index[0], get_today())
    # #     df = df.reindex(all_dates).fillna(0)
    # #     self._cashflow = df
    # #     df = df[['cashflow']]
    # #     self.cashflow = df
    # #     return df


    # # def get_timeseries_cash(self):
    # #     df = self.frame
    # #     df['usdkrw'] = df.index.map(get_usdkrw_of_date)
    # #     df['initial_balance_usd'] = df['initial_balance_usd'].ffill()
    # #     df['initial_balance_krw'] = df['initial_balance_usd'] * df['usdkrw']
    # #     df = df.merge(self.cashflow, left_index=True, right_index=True, how='left')
    # #     df['cashflow'] = df['cashflow'].fillna(0)
    # #     df['cashflow_cum'] = df['cashflow'].cumsum()
    # #     df['cash_krw'] = df['initial_balance_krw'] + df['cashflow_cum']
    # #     df['cash_usd'] = df['cash_krw'] / df['usdkrw']
    # #     cols_to_keep = ['cash_usd', 'cash_krw']
    # #     df = df[cols_to_keep]
    # #     self.cash = df
    # #     return df
    
    # # def get_timeserise_of_stock(self):
    # #     df = self.valuation.copy()
    # #     df['usdkrw'] = df.index.map(get_usdkrw_of_date)
    # #     df['stock_krw'] = df['valuation']
    # #     df['stock_usd'] = df['stock_krw'] / df['usdkrw']
    # #     cols_to_keep = ['stock_usd', 'stock_krw']
    # #     df = df[cols_to_keep]
    # #     self.stock = df
    # #     return df

    # def get_timeseries(self):
    #     df = self.cash.merge(self.stock, how='left', left_index=True, right_index=True).fillna(0)
    #     df['nav_usd'] = df['cash_usd'] + df['stock_usd']
    #     df['nav_krw'] = df['cash_krw'] + df['stock_krw']
    #     df['price_usd'] = df['nav_usd']/df['nav_usd'].iloc[0]*1000
    #     df['price_krw'] = df['nav_krw']/df['nav_krw'].iloc[0]*1000
    #     df['weight_cash_usd'] = 100 * df['cash_usd'] / df['nav_usd']
    #     df['weight_stock_krw'] = 100 * df['stock_krw'] / df['nav_krw']
    #     df['weight_cash_krw'] = 100 * df['cash_krw'] / df['nav_krw']
    #     df['weight_stock_usd'] = 100 * df['stock_usd'] / df['nav_usd']
    #     df['return_usd'] = df['price_usd'].pct_change().fillna(0) * 100
    #     df['return_krw'] = df['price_krw'].pct_change().fillna(0) * 100
    #     df['cumreturn_usd'] = (df['price_usd']/df['price_usd'].iloc[0] - 1)*100
    #     df['cumreturn_krw'] = (df['price_krw']/df['price_krw'].iloc[0] - 1)*100
    #     self.df = df
    #     return df