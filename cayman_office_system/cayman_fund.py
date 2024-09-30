import pandas as pd
import matplotlib.pyplot as plt
from shining_pebbles import get_today
from .dataset_constants import *
from .dataset_loader import *
from .holdings import Holdings
from .finance_utils import get_last_day_of_month
from .market_information import get_ks_equity_info
    
class CaymanFund:
    def __init__(self, date=None):
        self.fund_name = 'LIFE KOREA ENGAGEMENT FUND'
        self.fund_code = 'LKEF'
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        self.file_folder = file_folder['balance']
        self.raw = self.open_raw_balance()
        self.initial = self.get_initial_data_of_month()
        self.frame = self.get_frame()
        self.holdings = self.import_holdings(self.date)
        self.tickers = self.holdings.tickers
        self.equities = get_ks_equity_info(self.tickers)
        self.trades = self.import_trades()

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

    def get_frame(self):
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
    
    def get_timeseries(self, currency=None):
        cash = self.cash
        eval = self.evaluation
        df = cash.merge(eval, how='outer', right_index=True, left_index=True)
        df['cash_usd'] = df['cash_usd'].ffill()
        df['cash_krw'] = df['cash_krw'].ffill()
        df['stock_usd'] = df['stock_usd'].fillna(0)
        df['stock_krw'] = df['stock_krw'].fillna(0)
        df['nav_usd'] = df['cash_usd'] + df['stock_usd']
        df['nav_krw'] = df['cash_krw'] + df['stock_krw']
        df['weight_cash_usd'] = 100 * df['cash_usd'] / df['nav_usd']
        df['weight_stock_usd'] = 100 * df['stock_usd'] / df['nav_usd']
        df['weight_cash_krw'] = 100 * df['cash_krw'] / df['nav_krw']
        df['weight_stock_krw'] = 100 * df['stock_krw'] / df['nav_krw']
        self.timeseries = df
        if isinstance(currency, str) and currency.upper() == 'USD':
            df = df.filter(regex='^(?!.*_krw)')
            self.timeseries_usd = df
        elif isinstance(currency, str) and currency.upper() == 'KRW':
            df = df.filter(regex='^(?!.*_usd)')
            self.timeseries_krw = df
        return df
    
    def get_timeseries_nav(self, currency=None):
        if not hasattr(self, 'timeseries'):
            self.get_timeseries()
        df = self.timeseries
        df_nav = df[['nav_krw', 'nav_usd']].copy()
        df_nav['price_krw'] = (df_nav['nav_krw'] / df_nav['nav_krw'].iloc[0])*1000
        df_nav['return_krw'] = df_nav['price_krw'].pct_change().fillna(0) * 100
        df_nav['cumreturn_krw'] = (df_nav['price_krw']/df_nav['price_krw'].iloc[0] - 1)*100
        df_nav['price_usd'] = (df_nav['nav_usd'] / df_nav['nav_usd'].iloc[0])*1000
        df_nav['return_usd'] = df_nav['price_usd'].pct_change().fillna(0) * 100
        df_nav['cumreturn_usd'] = (df_nav['price_usd']/df_nav['price_usd'].iloc[0] - 1)*100
        cols_orderd = ['nav_krw', 'nav_usd', 'price_krw', 'price_usd', 'return_krw', 'return_usd', 'cumreturn_krw', 'cumreturn_usd']
        df_nav = df_nav[cols_orderd]
        self.nav = df_nav
        if isinstance(currency, str) and currency.upper() == 'USD':
            df_nav = df_nav.filter(regex='^(?!.*_krw)')
            self.nav_usd = df_nav
        elif isinstance(currency, str) and currency.upper() == 'KRW':
            df_nav = df_nav.filter(regex='^(?!.*_usd)')
            self.nav_krw = df_nav
        return df_nav

    def get_df(self):
        if not hasattr(self, 'holdings'):
            self.import_holdings(self.date)
        holdings = self.holdings
        df = holdings.pl.copy()
        nav_krw_latest = self.timeseries.iloc[-1].nav_krw
        df['weight'] = 100 * df['evaluation'] / nav_krw_latest
        df['attribution'] = df['return'] * df['weight'] / 100
        df = df.sort_values('weight', ascending=False)
        self.df = df
        return df
    
    def get_latest_info(self):
        row_nav = self.nav.iloc[-1:, :].reset_index()
        nav_latest_krw = row_nav.filter(regex='^(?!.*_usd)').to_dict(orient='records')[0]
        nav_latest_usd = row_nav.filter(regex='^(?!.*_krw)').to_dict(orient='records')[0]
        row_ts = self.timeseries.iloc[-1:, :]
        ts_latest_krw = row_ts.filter(regex='^(?!.*_usd)').to_dict(orient='records')[0]
        ts_latest_usd = row_ts.filter(regex='^(?!.*_krw)').to_dict(orient='records')[0]
        latest_krw = {**nav_latest_krw, **ts_latest_krw}
        latest_usd = {**nav_latest_usd, **ts_latest_usd}
        self.latest = {'krw': latest_krw, 'usd': latest_usd}
        return self.latest

    
    def get_df_latest(self):
        if not hasattr(self, 'latest'):
            self.get_latest_info()
        srs_latest = self.latest
        df = pd.DataFrame(srs_latest).T
        self.df_latest = df
        return df
        
    def get_pl_and_weights(self):
        if not hasattr(self, 'df'):
            self.get_df()
        df = self.df
        df_pl = get_df_pl(df)
        df_weights = get_df_weights(df)
        self.pl = df_pl
        self.weights = df_weights
        return df_pl, df_weights


def get_df_pl(df): 
    cols_pl = ['name', 'num_shares', 'price_average', 'price_last', 'evaluation', 'pl']
    pl = df[cols_pl]
    return pl

def get_df_weights(df):
    cols_weights = ['name', 'return', 'weight', 'attribution']
    df_weights = df[cols_weights]
    return df_weights



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