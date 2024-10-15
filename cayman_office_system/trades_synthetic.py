from .trade_synthetic import *

class SyntheticTrades:
    def __init__(self, data_sellbuys):
        self.trades = [SyntheticTrade(trade_data=data_sellbuy) for data_sellbuy in data_sellbuys]
        self.df = self.get_df()

    def get_df(self):
        trades = self.trades
        dfs = [trade.df for trade in trades]
        df = pd.concat(dfs, axis=0)
        df = df.reset_index(drop=True)
        return df
    