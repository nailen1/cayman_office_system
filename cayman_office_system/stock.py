from .data_mapping import *
from shining_pebbles import get_date_range, get_today
from .birdeye_connector import get_price_by_ticker
from .finance_utils import format_integer
import matplotlib.pyplot as plt
import numpy as np

class Stock:
    def __init__(self, df):
        self.raw = self.set_canonical_df(df)
        self.ticker = self.get_ticker()
        self.name = self.get_name()
        self.df = self.get_df()
        self.valuation = self.get_valuation()
        self.latest = self.get_holding_info()

    def set_canonical_df(self, df):
        cols_of_caninical_stock_data = ['date', 'ticker', 'name', 'type', 'num_shares', 'price_trade', 'amount_trade', 'delta_shares', 'cashflow', 'cashflow_cum',
                                        'num_shares_cum', 'price_average']
        for col in df.columns:
            if col not in cols_of_caninical_stock_data:
                raise ValueError(f'Column {col} is not in the canonical stock data')
        self.raw = df
        return df

    def get_ticker(self):
        return self.raw.iloc[0]['ticker']
    
    def get_name(self):
        return self.raw.iloc[0]['name']

    def get_df(self):
        df = self.raw
        cols_to_keep = ['date', 'ticker', 'name', 'num_shares_cum', 'price_trade', 'price_average']
        df_nums = df[cols_to_keep].copy()
        df_nums.loc[:, 'total_amount'] = df_nums['num_shares_cum'] * df_nums['price_average']
        df_nums.loc[:, 'realization'] = df['cashflow'].apply(lambda x: x if x > 0 else 0)
        df_all_dates = pd.DataFrame({'date': get_date_range(start_date_str=df_nums['date'].min(), end_date_str=get_today())})
        timeseries = pd.merge(df_all_dates, df_nums, on='date', how='left').sort_values('date')
        timeseries['realization'] = timeseries['realization'].fillna(0)
        timeseries = timeseries.ffill()
        price_of_ticker = get_price_by_ticker(ticker=self.ticker)
        timeseries = timeseries.merge(price_of_ticker, how='left', left_on='date', right_index=True).ffill()
        timeseries['valuation'] = timeseries['num_shares_cum'] * timeseries['price_last']
        timeseries['pl'] = timeseries['valuation'] - timeseries['total_amount']
        self.df = timeseries
        return timeseries
    
    def get_holding_info(self, date=None):
        if not hasattr(self, 'df'):
            self.get_df()
        df = self.df
        if date == None:
            info = df.iloc[[-1]]
        else:
            info = df[df['date'] == date]
        self.info = info
        return info
    
    def plot(self):
        if not hasattr(self, 'df'):
            self.get_df()
        plot_stock(self.df, self.ticker, self.name)
        return None

    def get_number(self):
        if not hasattr(self, 'df'):
            self.get_df()
        df = self.df.copy()
        df = df[['date', 'num_shares_cum']].set_index('date')
        df = df[~df.index.duplicated(keep='last')]
        df['num_shares_cum'] = df['num_shares_cum'].map(format_integer)
        df.columns = [f'num_shares: {self.ticker}']
        self.number = df
        return df
    
    def get_amount(self):
        if not hasattr(self, 'df'):
            self.get_df()
        df = self.df.copy()
        df = df[['date', 'total_amount']].set_index('date')
        df = df[~df.index.duplicated(keep='last')]
        df.columns = [f'total_amount: {self.ticker}']
        self.amount = df
        return df

    def get_valuation(self):
        if not hasattr(self, 'df'):
            self.get_df()
        df = self.df.copy()
        df = df[['date', 'valuation']].set_index('date')
        df = df[~df.index.duplicated(keep='last')]
        df.columns = [f'valuation: {self.ticker}']
        self.valuation = df
        return df

def plot_stock(df, ticker=None, name=None):
    ticker, name = df.iloc[0]['ticker'], df.iloc[0]['name']
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.fill_between(df['date'], df['total_amount'], color='skyblue', alpha=0.4, label='Total Amount', where=np.isfinite(df['total_amount']))    
    bars = ax1.bar(df['date'], df['realization'], color='lightcoral', alpha=0.6, label='Realization')

    for bar in bars:
        height = bar.get_height()
        if height > 0:  # Only label positive bars
            ax1.text(bar.get_x() + bar.get_width() / 2, height, f'{height:.2e}', ha='center', va='bottom', color='black', fontsize=10)

    ax1.plot(df['date'], df['valuation'], color='orange', label='Valuation', linewidth=2)    
    ax1.plot(df['date'], df['pl'], color='gray', label='P/L', linewidth=1)
    ax1.set_ylabel('KRW', color='black')
    plt.xticks(rotation=90, ha='center')

    ax1.grid(True, which='both', linestyle='--', linewidth=0.5, color='gray', alpha=0.6)
    title_suffix = f': {name} ({ticker})' if name and ticker else ''
    fig.suptitle(f'Stock Valuation'+title_suffix, fontsize=16, color='black')
    ax1.legend(loc='upper left')
    plt.tight_layout()

    plt.show()

    return None

