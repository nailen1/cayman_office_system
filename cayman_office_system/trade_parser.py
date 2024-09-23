from .dataset_constants import file_folder
from .trade_utils import *
from .birdeye_connector import get_df_timeseries_by_ticker
from .cayman_office_system import *
import pandas as pd


class Trade:
    def __init__(self, date=None, index=None, file_folder=file_folder['trade']):
        self.file_folder = file_folder
        self.date = date
        self.index = index
        self.set_date_and_index(date, index)
        self.raw = self.open_raw()
        self.transactions = self.get_transactions()
        self.df = self.show_info()
        self.data = self.get_data()
        self.timeseries = self.get_timeseries_of_trade_since_trade()
        self.amount = {'date': self.date, 'amount_order': self.net_amount}


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

        print(f'- set (date, index) = ({self.date}, {self.index})')
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
    
    def show_info(self):
        trxs = self.transactions
        data = [trx.data for trx in trxs]
        df = pd.DataFrame(data)
        self.tickers = list(df['ticker'])
        self.names = list(df['name'])
        self.net_amount = df['net_amount'].sum()
        self.info = df
        print(f'- net_amount: {self.net_amount}')
        return df
    
    def get_data(self):
        if not hasattr(self, 'info'):
            self.show_info()
        df= self.info
        data = df[['date', 'ticker', 'num_shares']].to_dict(orient='records')
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

    
class Transaction:
    def __init__(self, raw, date):
        self.raw = raw
        self.date = date
        self.data_raw = self.get_data()
        self.get_properties()
        self.ticker = self.get_ticker()
        self.df = self.get_df()
        self.data = self.get_data_calculated()
        
    def get_data(self):
        data_raw = get_data_in_transaction(self.raw, KEYS_TRANSACTION)
        self.data_raw = data_raw
        return data_raw
    
    def get_properties(self):
        data = self.data_raw
        self.type = get_type_in_transaction(data)
        self.name = get_name_in_transaction(data)
        self.isin_code = get_isin_code_in_transaction(data)
        self.abbr_code = get_abbr_code_in_transaction(data)
        self.consideration = get_consideration_in_transaction(data)
        self.commission = get_commission_in_transaction(data)
        self.sales_tax = get_sales_tax_in_transaction(data)
        self.capital_gain_tax = get_capital_gains_tax_in_transaction(data)
        self.net_amount = get_net_amount_in_transaction(data)
        self.num_shares = get_total_no_of_shares_in_transaction(data)
        self.average_price = get_average_price_in_transaction(data)
        return self
    
    def get_ticker(self):
        ticker = get_ticker_in_transaction(self.data_raw)
        self.ticker = ticker
        return ticker
    
    def get_df(self):
        df = get_df_sellbuy(self.raw)
        df['date'] = self.date
        df['ticker'] = self.ticker
        df['amount'] = df['num_shares'] * df['price_executed']
        cols_ordered = ['date', 'ticker', 'num_shares', 'currency', 'price_executed', 'amount']
        df = df[cols_ordered]
        df = df.set_index('date')
        self.df = df
        return df
    
    def get_num_shares_calculated(self):
        num_shares = self.df['num_shares'].sum()
        self.num_shares_calculated = num_shares
        return num_shares

    def get_consideration_calculated(self):
        consideration = self.df['amount'].sum()
        self.consideration_calculated = consideration
        return consideration
    
    def get_commission_calculated(self, rnd=True):
        consideration = self.consideration_calculated
        commission = consideration/1000
        if rnd:
            commission = round(commission)
        self.commission_calculated = commission
        return commission
    
    def get_average_price_calculated(self):
        if not hasattr(self, 'consieration_calculated'):
            self.get_consideration_calculated()
        if not hasattr(self, 'num_shares_calculated'):
            self.get_num_shares_calculated()
        average_price = self.consideration_calculated/self.num_shares_calculated
        self.average_price_calculated = average_price
        return average_price
    
    def get_net_amount_calculated(self):
        if not hasattr(self, 'consieration_calculated'):
            self.get_consideration_calculated()
        if not hasattr(self, 'commission_calculated'):
            self.get_commission_calculated()
        net_amount = self.consideration_calculated + self.commission_calculated
        self.net_amount_calculated = net_amount
        return net_amount
    
    def get_data_calculated(self):
        if not hasattr(self, 'df'):
            self.get_df()
        self.get_num_shares_calculated()
        self.get_consideration_calculated()
        self.get_commission_calculated()
        self.get_average_price_calculated()
        self.get_net_amount_calculated()

        data = {
            'date': self.date,
            'name': self.name,
            'ticker': self.ticker,
            'type': self.type,
            'num_shares': self.num_shares_calculated,
            'average_price': self.average_price_calculated,
            'consideration': self.consideration_calculated,
            'commission': self.commission_calculated,
            'net_amount': self.net_amount_calculated
        }
        self.data = data
        return data

    def check_identity_of_data(self):
        print(f'Check Identity of Data: (system) == (calculated)')
        if self.num_shares == self.num_shares_calculated:
            print(f'- num_shares: {self.num_shares} == {self.num_shares_calculated}')
        else:
            print(f'- num_shares: {self.num_shares} != {self.num_shares_calculated}')
        if self.consideration == self.consideration_calculated:
            print(f'- consideration: {self.consideration} == {self.consideration_calculated}')
        else:    
            print(f'- consideration: {self.consideration} != {self.consideration_calculated}')
        if self.commission == self.commission_calculated:
            print(f'- commission: {self.commission} == {self.commission_calculated}')
        else:
            print(f'- commission: {self.commission} != {self.commission_calculated}')
        if self.average_price == self.average_price_calculated:
            print(f'- average_price: {self.average_price} == {self.average_price_calculated}')
        else:
            print(f'- average_price: {self.average_price} != {self.average_price_calculated}')
        if self.net_amount == self.net_amount_calculated:
            print(f'- net_amount: {self.net_amount} == {self.net_amount_calculated}')
        else:
            print(f'- net_amount: {self.net_amount} != {self.net_amount_calculated}')
        return None
    
    def show_info(self):
        self.check_identity_of_data()
        print(self.get_data_calculated())
        return self.df


def get_df_timeseries_of_a_ticker_in_a_trade(trade_data):
    date = trade_data['date']
    ticker = trade_data['ticker']
    num_shares = trade_data['num_shares']
    df = get_df_timeseries_by_ticker(ticker=ticker)
    df = df[df.index >= date]
    df['num_shares'] = num_shares
    df[f'{ticker}: ({date}, {num_shares})'] = df['price_last'] * df['num_shares']
    return df
