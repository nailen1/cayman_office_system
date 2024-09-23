import pandas as pd
from shining_pebbles import get_today
from .trade_utils import get_dates_of_trades_in_file_folder
from .trade_parser import Trade
from .birdeye_connector import get_price_of_date_by_ticker
from .market_information import append_market_info_to_df


class Trades:
    def __init__(self, start_date=None, end_date=None):
        self.dates = self.set_period(start_date=start_date, end_date=end_date)
        self.trades = self.get_trades()
        self.history = self.get_history()
        self.pl = self.get_pl()
        self.timeseries_amount = self.get_timeseries_of_amount()
        self.timeseries_evaluation = self.get_timeseries_of_evaluation()

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
    
    def get_history(self):
        dfs = [trade.info for trade in self.trades]
        df = pd.concat(dfs, axis=0)
        df = df.reset_index(drop=True)
        self.history = df
        return df
    
    def get_pl(self):
        df = self.history.copy()
        df = append_market_info_to_df(df)
        cols_to_keep = ['date', 'ticker', 'name_y', 'num_shares', 'average_price', 'price_last', 'net_amount']
        df = df[cols_to_keep]
        df['evaluation'] = df['num_shares'] * df['price_last']
        df['pl'] = df['evaluation'] - df['net_amount']
        df['return'] = (df['price_last'] / df['average_price'] - 1)*100
        df = df.rename(columns={'name_y': 'name', 'average_price': 'price_average'})
        self.pl = df
        return df
    
    def get_evaluation(self):
        df = self.pl.copy()[['date', 'ticker', 'num_shares', 'evaluation']]
        df['price_of_date'] = df.apply(lambda row: get_price_of_date_by_ticker(ticker=row['ticker'], date=row['date']), axis=1)
        df['evaluation_of_date'] = df['num_shares'] * df['price_of_date']
        df_evaluation = df[['date', 'evaluation_of_date', 'evaluation']].groupby('date').sum()
        df_evaluation = df_evaluation.rename(columns={'evaluation': 'evaluation_of_today'})
        self.evaluation = df_evaluation
        self.total_evaluation_of_today = {'date': get_today(), 'total_evaluation': df_evaluation['evaluation_of_today'].sum()}
        return df_evaluation
    
    def get_timeseries_of_amount(self):
        trades = self.trades
        amounts = [trade.amount for trade in trades]
        df = pd.DataFrame(amounts)
        df = df.set_index('date').sort_index()
        df['total_amount_order'] = df['amount_order'].cumsum()
        self.timeseries_amount = df
        return df
    
    def get_timeseries_of_evaluation(self):
        trades = self.trades
        dfs = [trade.timeseries.iloc[:, -2:-1] for trade in trades]
        df = pd.concat(dfs, axis=1)
        df = df.fillna(0)
        df['total_evaluation'] = df.sum(axis=1)
        self.timeseries_evaluation = df
        return df
