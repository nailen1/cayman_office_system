from cayman_office_system.cos_dataset_loader import open_df_order_by_date, open_df_order_latest, open_df_order_by_index, preprocess_df_order
from cayman_office_system.market_database import get_ks_equity_info
from cayman_office_system.finance_utils import get_ticker_bbg_of_ticker
from .order_utils import get_dates_of_order_in_file_folder, append_market_info_to_sheet, append_derived_info_to_sheet
from .order_consts import COLUMNS_INFO_ORDER

class Order:
    def __init__(self, date=None, index=None):
        self.date = date
        self.index = index
        self.raw = self.open_raw_order_sheet()
        self.get_order_date()
        self.df = self.get_sheet()
        self.tickers = self.get_tickers()
        self.tickers_bbg = self.get_tickers_bbg()
        self.equities = self.get_equity_info()
        self.info = self.get_info()

    def open_raw_order_sheet(self):
        if self.date != None:
            df = open_df_order_by_date(self.date)
        elif self.index == None:
            self.index = -1
            df = open_df_order_latest()
        else:
            df = open_df_order_by_index(index=self.index)
        self.raw = df
        return df
    
    def get_order_date(self):
        if self.date != None:
            date = self.date
        else:
            date = get_dates_of_order_in_file_folder()[self.index]
        print(f'- order date: {date}')
        self.date = date
        return date   
    
    def get_params(self):
        dct = {
            'date': self.date,
            'index': self.index
        }
        self.get_params = dct
        return dct
    
    def get_sheet(self):
        df = self.raw
        df = preprocess_df_order(df)
        self.sheet = df
        return df
    
    def get_tickers(self):
        df = self.sheet
        tickers = list(df['ticker'])
        self.tickers = tickers
        return tickers

    def get_tickers_bbg(self):
        tickers = self.get_tickers()
        tickers_bbg = [get_ticker_bbg_of_ticker(ticker) for ticker in tickers]
        self.tickers_bbg = tickers_bbg
        return tickers_bbg 
    
    def get_equity_info(self):
        tickers = self.tickers
        df = get_ks_equity_info(tickers)
        self.equities = df
        return df
    
    def get_info(self):
        df = self.sheet
        df = append_market_info_to_sheet(df)
        df = append_derived_info_to_sheet(df)
        cols_ordered = COLUMNS_INFO_ORDER
        df = df[cols_ordered]
        df = df.set_index('date')        
        self.info = df
        return df
    