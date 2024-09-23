import pandas as pd
import matplotlib.pyplot as plt
from shining_pebbles import get_today
from .dataset_constants import *
from .dataset_loader import *
from .holdings import Holdings
from .finance_utils import get_last_day_of_month
from .market_information import get_ks_equity_info
    
class Account:
    def __init__(self, date=None):
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        self.file_folder = file_folder['balance']
        self.raw = self.open_raw_balance()
        self.initial = self.get_initial_data_of_month()
        self.frame = self.get_account_frame()
        self.holdings = self.import_holdings(self.date)
        self.tickers = self.holdings.tickers
        self.equities = get_ks_equity_info(self.tickers)
        self.trades = self.import_trades()
        self.cash = self.get_timeseries_cash()
        self.stock = self.get_timeseries_stock()
        self.nav = self.get_timeseries_nav()
        self.weights = self.get_weights()
        self.latest = self.get_latest_info()

    def open_raw_balance(self):
        df = open_balance_of_month(month=self.month, file_folder=self.file_folder)
        self.raw = df
        return df

    def get_initial_data_of_month(self):
        df = self.raw
        initial_data = get_data_balance(df)
        self.initial = initial_data
        self.initial_date = initial_data['date']
        self.initial_balance = initial_data['available_balance']
        return initial_data

    def get_account_frame(self):
        initial_date = self.initial_date
        initial_balance_usd = self.initial_balance
        # initial_usdkrw = get_usdkrw_of_date(date=initial_date)
        # initial_balance_krw = initial_balance_usd * initial_usdkrw
        frame_data = {'date': initial_date, 'initial_balance_usd': initial_balance_usd}
        frame = pd.DataFrame(data=frame_data, index=[0])
        frame = frame.set_index('date')
        self.frame = frame
        return frame
    
    def import_holdings(self, date):
        end_date = get_last_day_of_month(date)
        holdings = Holdings(end_date=end_date)
        self.holdings = holdings
        return holdings
    
    def import_trades(self):
        if not hasattr(self, 'holdings'):
            self.import_holdings(self.date)
        holdings = self.holdings
        trades = holdings.trades
        self.trades = trades
        return trades
    
    def get_timeseries_cash(self):
        frame = self.frame
        amounts = self.trades.timeseries_amount
        df = frame.merge(amounts[['amount_order']], how='outer', left_index=True, right_index=True)
        df['initial_balance_usd'] = df['initial_balance_usd'].ffill()
        df['amount_order'] = df['amount_order'].fillna(0)
        df['usdkrw'] = df.index.map(get_usdkrw_of_date)
        df['amount_usd'] = df['amount_order'] / df['usdkrw']
        df['cash_usd'] = df['initial_balance_usd'] - df['amount_usd'].cumsum()
        df['cash_krw'] = df['cash_usd'] * df['usdkrw']
        cols_to_keep = ['cash_usd', 'cash_krw']
        df = df[cols_to_keep]
        self.cash = df
        return df

    def get_timeseries_stock(self):
        df = self.trades.timeseries_evaluation
        df['usdkrw'] = df.index.map(get_usdkrw_of_date)
        df['stock_krw'] = df['total_evaluation']
        df['stock_usd'] = df['stock_krw'] / df['usdkrw']
        cols_to_keep = ['stock_usd', 'stock_krw']
        df = df[cols_to_keep]
        self.evaluation = df
        return df
    
    def get_timeseries_nav(self, currency=None):
        cash = self.cash
        eval = self.evaluation
        df = cash.merge(eval, how='outer', right_index=True, left_index=True)
        df['cash_usd'] = df['cash_usd'].ffill()
        df['cash_krw'] = df['cash_krw'].ffill()
        df['stock_usd'] = df['stock_usd'].fillna(0)
        df['stock_krw'] = df['stock_krw'].fillna(0)
        df['nav_usd'] = df['cash_usd'] + df['stock_usd']
        df['nav_krw'] = df['cash_krw'] + df['stock_krw']
        df['weight_cash'] = 100 * df['cash_usd'] / df['nav_usd']
        df['weight_stock'] = 100 * df['stock_usd'] / df['nav_usd']
        if isinstance(currency, str) and currency.upper() == 'USD':
            df = df.filter(regex='^(?!.*_krw)')
        elif isinstance(currency, str) and currency.upper() == 'KRW':
            df = df.filter(regex='^(?!.*_usd)')
        self.nav = df
        return df

    def get_weights(self):
        if not hasattr(self, 'holdings'):
            self.import_holdings(self.date)
        holdings = self.holdings
        df = holdings.pl.copy()
        nav_krw_latest = self.nav.iloc[-1].nav_krw
        df['weight'] = 100 * df['evaluation'] / nav_krw_latest
        df['attribution'] = df['return'] * df['weight'] / 100
        df = df.sort_values('weight', ascending=False)
        self.weights = df
        return df
    
    def get_latest_info(self):
        srs_latest = self.nav.iloc[-1].copy()
        if not hasattr(self, 'weights'):
            self.get_weights()
        df = self.weights
        total_net_amount = df['net_amount'].sum()
        total_evaluation = df['evaluation'].sum()
        total_pl = df['pl'].sum()
        total_attribution = df['attribution'].sum()
        srs_latest['total_net_amount'] = total_net_amount
        srs_latest['total_evaluation'] = total_evaluation
        srs_latest['total_pl'] = total_pl
        srs_latest['total_return'] = total_attribution
        self.latest = srs_latest
        return srs_latest
    
    def get_df_latest(self):
        if not hasattr(self, 'latest'):
            self.get_latest_info()
        srs_latest = self.latest
        df = pd.DataFrame(srs_latest).T
        self.df_latest = df
        return df
        
    def get_dfs_concise(self):
        if not hasattr(self, 'weights'):
            self.get_weights()
        df_weights = self.weights
        df_absolutes = get_df_absolutes(df_weights)
        df_portions = get_df_portions(df_weights)
        dfs = {'absolutes': df_absolutes, 'portions': df_portions}
        self.dfs = dfs
        return dfs


def get_df_absolutes(df_weights): 
    cols_absolutes = ['name', 'num_shares', 'price_last', 'evaluation', 'pl']
    df = df_weights[cols_absolutes]
    return df

def get_df_portions(df_weights):
    cols_portions = ['name', 'return', 'weight', 'attribution']
    df = df_weights[cols_portions]
    return df




def plot_nav(df_nav):
    df = df_nav

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.fill_between(df.index, 0, df['cash_krw'], label='Cash', alpha=0.5)
    ax.fill_between(df.index, df['cash_krw'], df['nav_krw'], label='Stock', alpha=0.5)

    ax.plot(df.index, df['nav_krw'], label='NAV', color='red', linewidth=2)

    ax.set_title('Cash, Stock, and NAV over time')
    ax.set_xlabel('Date')
    ax.set_ylabel('Amount (KRW)')
    ax.legend(loc='lower right')

    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x/1e9), ',')))

    ax.grid(True, linestyle='--', alpha=0.7)

    plt.xticks(rotation=45)
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.show()

    return None