import pandas as pd
import matplotlib.pyplot as plt
from shining_pebbles import get_today, get_date_range
from .dataset_constants import *
from .dataset_loader import *
from .timeseries import Timeseries
from .holdings import Holdings
    
class CaymanFund:
    def __init__(self, trades, date=None):
        self.fund_name = 'LIFE KOREA ENGAGEMENT FUND'
        self.fund_code = 'LKEF'
        self.date = date or get_today()
        self.month = self.date.replace('-', '')[4:6] 
        self.trades = trades
        self._timeseries = Timeseries(trades=self.trades)
        self._holdings = Holdings(trades=self.trades)
        self.tickers = self._holdings.tickers
        self.equities = self._holdings.equities
        self.timeseries_cash = self._timeseries.cash
        self.timeseries_stock = self._timeseries.stock
        self.timeseries = self._timeseries.df
    

    def get_timeseries_cash(self):
        frame = self.frame
        amounts = self.trades.timeseries_amount
        df = frame.merge(amounts[['amount_order']], how='outer', left_index=True, right_index=True)
        all_dates = get_date_range(self.initial_date, get_today())
        df = df.reindex(all_dates)
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
        self.stock = df
        return df
    
    def get_timeseries(self, currency=None):
        if not hasattr(self, 'cash'):
            self.get_timeseries_cash()
        cash = self.cash
        if not hasattr(self, 'stock'):
            self.get_timeseries_stock()
        stock = self.stock
        df = cash.merge(stock, how='outer', right_index=True, left_index=True)
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
        if isinstance(currency, str) and currency.upper() == 'ALL':
            df_usd = df.filter(regex='^(?!.*_krw)')
            self.timeseries_usd = df_usd
            df_krw = df.filter(regex='^(?!.*_usd)')
            self.timeseries_krw = df_krw
        elif isinstance(currency, str) and currency.upper() == 'USD':
            df = df.filter(regex='^(?!.*_krw)')
            self.timeseries_usd = df
        elif isinstance(currency, str) and currency.upper() == 'KRW':
            df = df.filter(regex='^(?!.*_usd)')
            self.timeseries_krw = df
        return df
    
    def get_timeseries_nav(self, currency=None):
        if not hasattr(self, 'timeseries'):
            self.get_timeseries(currency=currency)
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
        if isinstance(currency, str) and currency.upper() == 'ALL':
            df_nav_usd = df_nav.filter(regex='^(?!.*_krw)')
            self.nav_usd = df_nav_usd
            df_nav_krw = df_nav.filter(regex='^(?!.*_usd)')
            self.nav_krw = df_nav_krw
        elif isinstance(currency, str) and currency.upper() == 'USD':
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
        if not hasattr(self, 'nav'):
            self.get_timeseries_nav()
        row_nav = self.nav.iloc[-1:, :].reset_index()
        nav_latest_krw = row_nav.filter(regex='^(?!.*_usd)').to_dict(orient='records')[0]
        nav_latest_usd = row_nav.filter(regex='^(?!.*_krw)').to_dict(orient='records')[0]
        if not hasattr(self, 'timeseries'):
            self.get_timeseries()
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