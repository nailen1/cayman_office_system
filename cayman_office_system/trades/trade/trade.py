from cayman_office_system.path_director import file_folder
from .trade_utils import (
    get_df_timeseries_of_a_ticker_in_a_trade,
    get_keys_from_df_trade,
    get_dates_of_trades_in_file_folder,
    open_df_trade_by_index,
    open_df_trade_by_date,
    get_data_indicies_of_keys,
    get_pair_index_of_info,
    get_pairs_index_of_transaction,
    get_df_raw_info,
    get_df_raw_transaction
)
from .transaction import Transaction

import pandas as pd


class Trade:
    def __init__(self, date=None, index=None, file_folder=file_folder['trade']):
        self.file_folder = file_folder
        self.date = date
        self.index = index
        self.set_date_and_index(date, index)
        self.raw = self.open_raw()
        self.transactions = self.get_transactions()
        self.df = self.get_df()
        self.data = self.get_data()
        self.timeseries = self.get_timeseries_of_trade_since_trade()
        self.trade_flow = {'date': self.date, 'flow': self.flow}


    def set_date_and_index(self, date, index):
        if date and index:
            raise ValueError('Both date and index are given.')
        elif date:
            self.date = date
            self.index = None
        elif index:
            self.index = index
            dates = get_dates_of_trades_in_file_folder(file_folder=self.file_folder, form='%Y-%m-%d')
            self.date = dates[self.index]
        else:
            self.index = -1
            dates = get_dates_of_trades_in_file_folder(file_folder=self.file_folder, form='%Y-%m-%d')
            self.date = dates[self.index]

        print(f'| import trade of (date, index) = ({self.date}, {self.index})')
        return self

    def open_raw(self):
        if self.index:
            df = open_df_trade_by_index(self.index, file_folder=self.file_folder)
        elif self.date:
            df = open_df_trade_by_date(self.date, file_folder=self.file_folder)
        self.raw = df
        return df
    
    def get_raw_info(self):
        df_info = get_df_raw_info(self.raw, self.pair_info)
        return df_info
    
    def get_raws_tranaction(self):
        self.keys = get_keys_from_df_trade(self.raw)
        self.indices = get_data_indicies_of_keys(self.keys, self.raw)
        self.pair_info = get_pair_index_of_info(self.indices)
        self.pairs_transaction = get_pairs_index_of_transaction(self.indices)
        dfs = [get_df_raw_transaction(self.raw, pair) for pair in self.pairs_transaction]
        self.raws_transaction = dfs        
        return dfs
    
    def get_transactions(self):
        if not hasattr(self, 'raws_transaction'):
            self.get_raws_tranaction()
        transactions = [Transaction(raw, self.date) for raw in self.raws_transaction]
        self.transactions = transactions
        return transactions
    
    def get_df(self):
        trxs = self.transactions
        data = [trx.data for trx in trxs]
        df = pd.DataFrame(data)
        # df = df.rename(columns={'average_price': 'price_trade_average', 'net_amount': 'net_amount_executed'})
        df['delta_shares'] = df.apply(lambda row: int(row['num_shares']) if row['type'] == 'Buy' else int(-row['num_shares']), axis=1)
        df['flow'] = df.apply(lambda row: -row['net_amount'] if row['type'] == 'Buy' else row['net_amount'], axis=1)
        self.tickers = list(df['ticker'])
        self.names = list(df['name'])
        self.net_amount = df['net_amount'].sum()
        self.flow = df['flow'].sum()
        # self.df = df
        print(f'|- cash flow by trading: {self.flow}')
        return df
    
    def get_data(self):
        if not hasattr(self, 'df'):
            self.get_df()
        df= self.df
        data = df[['date', 'ticker', 'type', 'num_shares', 'delta_shares']].to_dict(orient='records')
        self.data = data
        return data
    
    def get_timeserieses_of_tickers_since_trade(self):
        if not hasattr(self, 'data'):
            self.get_data()
        dfs = [get_df_timeseries_of_a_ticker_in_a_trade(trade_data) for trade_data in self.data]
        self.timeserieses = dfs
        return dfs
    
    def get_timeseries_of_trade_since_trade(self):
        if not hasattr(self, 'timeserieses'):
            self.get_timeserieses_of_tickers_since_trade()
        dfs = [df.iloc[:, -1:] for df in self.timeserieses]
        df = pd.concat(dfs, axis=1)
        df[f'evaluation: {self.date}'] = df.sum(axis=1)
        df[f'cumreturn: {self.date}'] = (df[f'evaluation: {self.date}'] / df[f'evaluation: {self.date}'].iloc[0] - 1)*100
        self.timeseries = df
        return df
