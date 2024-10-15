import pandas as pd
from shining_pebbles import get_today, get_date_range
from .finance_utils import get_ticker_bbg_of_ticker
from .trade_utils import get_dates_of_trades_in_file_folder
from .trade_parser import Trade
from .birdeye_connector import get_price_of_date_by_ticker
from .market_information import append_market_info_to_df
from .trade_synthetic_data import DNDONGA_MERGER
from .trades_synthetic import SyntheticTrades


class Trades:
    def __init__(self, start_date=None, end_date=None):
        self.dates = self.set_period(start_date=start_date, end_date=end_date)
        self.trades = self.get_trades()
        self.merged_info = self.get_merged_info()
        self.history = self.get_history()
        self.pl_buys = self.get_pl_evaluated_of_buys()
        self.pl_sells = self.get_pl_realized_of_sells()
        # self.timeseries_cash_flow = self.get_timeseries_of_cash_flow()
        # self.timeseries_evaluation = self.get_timeseries_of_evaluation()

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
    
    def get_merged_info(self, synthetic_trades=True):
        dfs = [trade.df for trade in self.trades]
        if synthetic_trades:
            dfs = dfs + [SyntheticTrades(data_sellbuys=DNDONGA_MERGER).df]        
        df = pd.concat(dfs, axis=0)
        df = df.reset_index(drop=True)
        df = df.sort_values(by='date', ascending=True)
        df_merged_info = append_market_info_to_df(df)
        df_merged_info = df_merged_info.rename(columns={'name': 'name_in_file', 'name_y': 'name', 'average_price': 'price_average'})
        self.merged_info = df_merged_info
        # ['date', 'name_x', 'ticker', 'type', 'num_shares', 'average_price',
        #        'consideration', 'commission', 'net_amount', 'delta_shares',
        #        'cash_flow', 'ticker_bbg', 'name_kr', 'name_y', 'market_index',
        #        'sector', 'cap(/1e8)', 'price_last']
        return df_merged_info


    def get_history(self):
        if not hasattr(self, 'merged_info'):
            self.get_merged_info()
        df = self.merged_info
        cols_to_keep = ['date', 'ticker', 'name', 'type', 'num_shares', 'price_average', 'net_amount', 'cash_flow']
        df = df[cols_to_keep]
        self.history = df
        return df
    
    def get_pl_evaluated_of_buys(self):
        if not hasattr(self, 'merged_info'):
            self.get_merged_info()        
        df = self.merged_info.copy()
        df = df[df['type'].isin(['Buy', 'Buy_synthetic'])]

        # DN exceptions before '2024-10-07'
        def adjust_price(row):
            if row['ticker'] == '007340 KS' and row['date'] <= '2024-10-07':
                return row['price_last'] * 5
            return row['price_last']
        
        df['price_last'] = df.apply(adjust_price, axis=1)
        df['evaluation'] = df['num_shares'] * df['price_last']
        df['pl'] = df['evaluation'] - df['net_amount']
        df['return'] = (df['price_last'] / df['price_average'] - 1)*100
        cols_to_keep = ['date', 'ticker', 'name', 'delta_shares', 'price_average', 'price_last', 'net_amount', 'evaluation', 'pl', 'return']
        df = df[cols_to_keep]
        self.pl_buys = df
        return df
    
    def get_pl_realized_of_sells(self):
        if not hasattr(self, 'merged_info'):
            self.get_merged_info()        
        df_ref = self.merged_info.copy()
        df = df_ref[df_ref['type'].isin(['Sell', 'Sell_synthetic'])]

        # DN exceptions before '2024-10-07'
        def adjust_price(row):
            if row['ticker'] == '007340 KS' and row['date'] <= '2024-10-07':
                return row['price_last'] * 5
            return row['price_last']

        def get_average_purchase_price(df, date, name):
            df_dated = df[df['date']<date]
            df_named = df_dated[df_dated['name']==name]
            total_net_amount, total_num_shares = df_named['net_amount'].sum(), df_named['num_shares'].sum()
            average_purchase_price = total_net_amount/total_num_shares
            return average_purchase_price
        
        df['average_purchase_price'] = df.apply(lambda row: get_average_purchase_price(df_ref, row['date'], row['name']), axis=1)
        df['price_last'] = df.apply(adjust_price, axis=1)
        df['evaluation'] = df['num_shares'] * df['average_purchase_price']
        df['realization'] = df['num_shares'] * df['price_last']
        df['pl'] = df['realization'] - df['evaluation']
        df['return'] = (df['price_last'] / df['average_purchase_price'] - 1)*100
        cols_to_keep = ['date', 'ticker', 'name', 'delta_shares', 'average_purchase_price', 'price_last', 'realization', 'pl', 'return']
        df = df[cols_to_keep]
        self.pl_sells = df
        return df

    
    def get_evaluation(self):
        df = self.pl.copy()[['date', 'ticker', 'num_shares', 'evaluation']]
        df['price_of_date'] = df.apply(lambda row: get_price_of_date_by_ticker(ticker=row['ticker'], date=row['date']), axis=1)
        df['evaluation_of_date'] = df['num_shares'] * df['price_of_date'] if df['type'] == 'Buy' else 0
        df_evaluation = df[['date', 'evaluation_of_date', 'evaluation']].groupby('date').sum()
        df_evaluation = df_evaluation.rename(columns={'evaluation': 'evaluation_of_today'})
        self.evaluation = df_evaluation
        self.total_evaluation_of_today = {'date': get_today(), 'total_evaluation': df_evaluation['evaluation_of_today'].sum()}
        return df_evaluation
    
    def get_timeseries_of_cash_flow(self):
        if not hasattr(self, 'trades'):
            self.get_trades()
        trades = self.trades
        cash_flow = [trade.cash_flow for trade in trades]
        df = pd.DataFrame(cash_flow)
        df = df.set_index('date').sort_index()
        df['total_cash_flow'] = df['cash_flow'].cumsum()
        self.timeseries_cash_flow = df
        return df
    
    # def get_timeseries_of_sells(self):

    
    def get_timeseries_of_evaluation(self):
        trade_objs = self.trades
        dfs = [trade.timeseries.iloc[:, -2:-1] for trade in trade_objs]
        df = pd.concat(dfs, axis=1)
        df = df.fillna(0)
        all_dates = get_date_range(self.dates[0], get_today())
        df = df.reindex(all_dates)
        df = df.ffill()
        df = df.fillna(0)

        df['total_evaluation'] = df.sum(axis=1)
        self.timeseries_evaluation = df
        return df

