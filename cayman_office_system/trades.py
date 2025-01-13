import pandas as pd
import numpy as np
from .trade_utils import get_dates_of_trades_in_file_folder
from .trade_parser import Trade
from .stock import Stock
from shining_pebbles import get_today, get_date_range
from .birdeye_connector import get_price_by_ticker
from .market_information import append_market_info_to_df
from .trade_synthetic_data import DNDONGA_MERGER
from .trades_synthetic import SyntheticTrades

class Trades:
    def __init__(self, start_date=None, end_date=None):
        self.dates = self.set_period(start_date=start_date, end_date=end_date)
        self.trades = self.get_trades()
        self._raw = self.get_raw()
        self.tickers = self.get_tickers()
        self.df = self.get_df()
        self.stock = self.get_stock_objs()
        self.buys = self.get_df_buys()
        self.sells = self.get_df_sells()
 
    def set_period(self, start_date=None, end_date=None):
        self.start_date = start_date
        self.end_date = end_date
        dates = get_dates_of_trades_in_file_folder()
        if start_date != None:
            dates = [date for date in dates if date >= start_date]
        if end_date != None:
            dates = [date for date in dates if date <= end_date]
        self.dates = dates
        return dates

    def get_trades(self):
        trades = [Trade(date=date) for date in self.dates]
        self.trades = trades
        return trades
    
    def get_raw(self, synthetic_trades=True):
        dfs = [trade.df for trade in self.trades]
        if synthetic_trades:
            dfs = dfs + [SyntheticTrades(data_sellbuys=DNDONGA_MERGER).df]        
        df = pd.concat(dfs, axis=0)
        df = df.reset_index(drop=True)
        df = df.sort_values(by='date', ascending=True)
        df_merged = append_market_info_to_df(df)
        df_merged = df_merged.rename(columns={'name': 'name_in_file', 'name_y': 'name', 'average_price': 'price_trade', 'net_amount': 'amount_trade'})
        self._raw = df_merged
        # ['date', 'name_x', 'ticker', 'type', 'num_shares', 'average_price',
        #        'consideration', 'commission', 'amount_trade', 'delta_shares',
        #        'cashflow', 'ticker_bbg', 'name_kr', 'name_y', 'market_index',
        #        'sector', 'cap(/1e8)', 'price_last']
        return df_merged
    
    def get_tickers(self):
        if not hasattr(self, '_raw'):
            self.get_raw()
        return list(self._raw['ticker'].unique())

    def get_df(self):
        if not hasattr(self, '_raw'):
            self.get_raw()
        df = self._raw
        cols_to_keep = ['date', 'ticker', 'name', 'type', 'num_shares', 'price_trade', 'amount_trade', 'delta_shares', 'cashflow']
        df = df[cols_to_keep]
        self.df = df        
        dct = {}
        for ticker in self.tickers:
            df_ticker = df[df['ticker']==ticker].copy()
            df_ticker = df_ticker.reset_index(drop=True)
            df_ticker.loc[:,'cashflow_cum'] = df_ticker['cashflow'].cumsum()
            df_ticker.loc[:,'num_shares_cum'] = df_ticker['delta_shares'].cumsum()
            df_ticker.loc[:,'price_average'] = df_ticker.apply(lambda row: round(-row['cashflow_cum']/row['num_shares_cum'],2) if row['num_shares_cum'] != 0 else 0, axis=1)
            dct[ticker] = df_ticker
        self.dfs = dct
        return df
    
    def get_stock_objs(self):
        dct = {}
        for ticker, df in self.dfs.items():
            stock_obj = Stock(df=df)
            dct[ticker] = stock_obj
        self.stock = dct
        return dct

    def get_df_buys(self):
        if not hasattr(self, '_raw'):
            self.get_raw()        
        df = self._raw.copy()
        df = df[df['type'].isin(['Buy', 'Buy_synthetic'])]

        # DN exceptions before '2024-10-07'
        def adjust_price(row):
            if row['ticker'] == '007340 KS' and row['date'] <= '2024-10-07':
                return row['price_last'] * 5
            return row['price_last']
        
        df['price_last'] = df.apply(adjust_price, axis=1)
        df['valuation'] = df['num_shares'] * df['price_last']
        df['pl'] = df['valuation'] - df['amount_trade']
        df['return'] = (df['price_last'] / df['price_trade'] - 1)*100
        cols_to_keep = ['date', 'ticker', 'name', 'delta_shares', 'price_trade', 'price_last', 'amount_trade', 'valuation', 'pl', 'return']
        df = df[cols_to_keep]
        self.buys = df
        return df
    
    def get_df_sells(self):
        if not hasattr(self, '_raw'):
            self.get_raw()        
        df_ref = self._raw.copy()
        df = df_ref[df_ref['type'].isin(['Sell', 'Sell_synthetic'])].copy()

        # DN exceptions before '2024-10-07'
        def adjust_price(row):
            if row['ticker'] == '007340 KS' and row['date'] <= '2024-10-07':
                return row['price_last'] * 5
            return row['price_last']

        def get_average_purchase_price(df, date, name):
            df_dated = df[df['date']<date]
            df_named = df_dated[df_dated['name']==name]
            total_net_amount, total_num_shares = df_named['amount_trade'].sum(), df_named['num_shares'].sum()
            average_purchase_price = total_net_amount/total_num_shares
            return average_purchase_price
        
        df['average_purchase_price'] = df.apply(lambda row: get_average_purchase_price(df_ref, row['date'], row['name']), axis=1)
        df['price_last'] = df.apply(adjust_price, axis=1)
        df['valuation'] = df['num_shares'] * df['average_purchase_price']
        df['realization'] = df['num_shares'] * df['price_last']
        df['pl'] = df['realization'] - df['valuation']
        df['return'] = (df['price_last'] / df['average_purchase_price'] - 1)*100
        cols_to_keep = ['date', 'ticker', 'name', 'delta_shares', 'average_purchase_price', 'price_last', 'valuation', 'realization', 'pl', 'return']
        df = df[cols_to_keep]
        self.sells = df
        return df
    

# def plot_stock(df, ticker=None, name=None):
#     ticker, name = df.iloc[0]['ticker'], df.iloc[0]['name']
#     fig, ax1 = plt.subplots(figsize=(10, 6))

#     ax1.fill_between(df['date'], df['total_amount'], color='skyblue', alpha=0.4, label='Total Amount', where=np.isfinite(df['total_amount']))    
#     bars = ax1.bar(df['date'], df['realization'], color='lightcoral', alpha=0.6, label='Realization')

#     for bar in bars:
#         height = bar.get_height()
#         if height > 0:  # Only label positive bars
#             ax1.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.2e}', ha='center', va='bottom', color='black', fontsize=10)

#     ax1.plot(df['date'], df['valuation'], color='orange', label='Valuation', linewidth=2)    
#     ax1.plot(df['date'], df['pl'], color='gray', label='P/L', linewidth=1)
#     ax1.set_ylabel('KRW', color='black')
#     plt.xticks(rotation=90, ha='center')

#     ax1.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', alpha=0.6)
#     title_suffix = f': {name} ({ticker})' if name and ticker else ''
#     fig.suptitle(f'Stock Valuation'+title_suffix, fontsize=16, color='black')
#     ax1.legend(loc='upper left')
#     plt.tight_layout()

#     plt.show()

#     return None

