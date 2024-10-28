from .trade_parser import *
from .trades import Trades
from .finance_utils import *
from .market_information import get_ks_equity_info, get_mapping_ks_name

class Holdings:
    def __init__(self, trades=None):
        self.trades = self.set_trades(trades)
        self._raw = self.get_raw()
        self.df = self.get_df()
        self.equities = get_ks_equity_info(self.tickers)

    def set_trades(self, trades=None):
        if trades is None:
            trades = Trades()
        self.trades = trades
        return trades

    def get_raw(self):
        buys = self.trades.buys[['ticker', 'delta_shares', 'net_amount', 'valuation']]
        buys.columns = ['ticker', 'delta_shares', 'action', 'stock']
        sells = self.trades.sells[['ticker', 'delta_shares', 'valuation', 'realization']]
        sells.columns = ['ticker', 'delta_shares', 'action', 'stock']
        sells.loc[:, 'action'] = -sells['action']
        sells.loc[:, 'stock'] = -sells['stock']    
        df = pd.concat([buys, sells], axis=0)
        self._raw = df
        return df
    
    def get_df(self):
        df = self._raw
        df = df.groupby('ticker').sum()
        df = df[df['delta_shares'] != 0]
        df['price_average'] = df['action'] / df['delta_shares']
        df['price_last'] = df['stock'] / df['delta_shares']
        df['ticker_bbg'] = df.index.map(get_ticker_bbg_of_ticker)
        df['name'] = df['ticker_bbg'].map(get_mapping_ks_name())
        cols_to_keep = ['name', 'delta_shares', 'price_average', 'price_last', 'action', 'stock']
        df = df[cols_to_keep]
        df['pl'] = df['stock'] - df['action']
        df['return'] = df['pl'] / df['action'] * 100
        df = df.rename(columns={'delta_shares': 'num_shares', 'action': 'total_amount', 'stock': 'total_valuation'})
        df = df.sort_values('total_valuation', ascending=False)
        self.tickers = df(df.index)
        self.names = df(df['name'])
        self.df = df
        return df
