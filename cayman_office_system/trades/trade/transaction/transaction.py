from cayman_office_system.market_database import get_ks_market_info
from .transaction_consts import KEYS_TRANSACTION
from .transaction_utils import (
    get_data_in_transaction, 
    get_type_in_transaction, 
    get_ticker_in_transaction, get_name_in_transaction, 
    get_consideration_in_transaction, 
    get_commission_in_transaction, 
    get_sales_tax_in_transaction, 
    get_capital_gains_tax_in_transaction, 
    get_net_amount_in_transaction, 
    get_total_no_of_shares_in_transaction, 
    get_average_price_in_transaction, 
    get_isin_code_in_transaction, get_abbr_code_in_transaction, 
    get_df_sellbuy
    )

class Transaction:
    def __init__(self, raw, date):
        self.raw = raw
        self.date = date
        self.data_raw = self.get_data_raw()
        self.data_in_transaction = self.get_properties()
        self.ticker = self.get_ticker()
        self.name_bbg = self.get_name_bbg()
        self.df = self.get_df()
        self.data = self.get_data()
        
    def get_data_raw(self):
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
        self.data_in_transaction = {
            'type': self.type,
            'name': self.name,
            'isin_code': self.isin_code,
            'abbr_code': self.abbr_code,
            'consideration': self.consideration,
            'commission': self.commission,
            'sales_tax': self.sales_tax,
            'capital_gain_tax': self.capital_gain_tax,
            'net_amount': self.net_amount,
            'num_shares': self.num_shares,
            'average_price': self.average_price
        }

        #    data_calculated = {
        #     'date': self.date,
        #     'name': self.name,
        #     'ticker': self.ticker,
        #     'type': self.type,
        #     'num_shares': self.num_shares_calculated,
        #     'average_price': self.average_price_calculated,
        #     'consideration': self.consideration_calculated,
        #     'commission': self.commission_calculated,
        #     'sales_tax': self.sales_tax,
        #     'net_amount': self.net_amount_calculated,
        # }
        return self.data_in_transaction
    
    def get_ticker(self):
        ticker = get_ticker_in_transaction(self.data_raw)
        self.ticker = ticker
        return ticker
    
    def get_name_bbg(self):
        ks = get_ks_market_info()
        name_bbg = ks.loc[f'{self.ticker} Equity']['name']
        self.name_bbg = name_bbg
        return name_bbg

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

        data_calculated = {
            'date': self.date,
            'name': self.name,
            'ticker': self.ticker,
            'type': self.type,
            'num_shares': self.num_shares_calculated,
            'average_price': self.average_price_calculated,
            'consideration': self.consideration_calculated,
            'commission': self.commission_calculated,
            'sales_tax': self.sales_tax,
            'net_amount': self.net_amount_calculated,
        }
        self.data_calculated = data_calculated
        return data_calculated
    
    def get_data(self):
        data = {
            'date': self.date,
            'name': self.name,
            'ticker': self.ticker,
            'type': self.type,
            'num_shares': self.num_shares,
            'average_price': self.average_price,
            'consideration': self.consideration,
            'commission': self.commission,
            'sales_tax': self.sales_tax,
            'net_amount': self.net_amount,
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

