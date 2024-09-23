from shining_pebbles import get_today
from .dataset_loader import *
from .finance_utils import get_ticker_bbg_of_ticker
from .market_information import get_ks_market_info, get_ks_equity_info
from .birdeye_connector import get_df_timeseries_by_ticker, get_price_of_date_in_df


COLUMNS_INFO_ORDER = ['date', 'ticker', 'name_kr', 'name', 'amount', 'price_acquisition_approx', 'price_last', 'return_approx', 'acquisition_approx_krw', 'evaluation_krw', 'acquisition_approx_usd', 'evaluation_usd', 'cap(/1e8)', 'sector', 'market_index']
COLUMNS_HOLDINGS = ['ticker', 'name_kr', 'name', 'amount', 'price_last', 'evaluation_krw', 'evaluation_usd', 'sector']


def append_market_info_to_sheet(sheet):
    sheet['ticker_bbg'] = sheet['ticker'].map(lambda x: get_ticker_bbg_of_ticker(x))
    ks = get_ks_market_info()
    df = sheet.reset_index().merge(ks, how='left', on='ticker_bbg')
    return df

def append_derived_info_to_sheet(df):
    ref_date = df.iloc[0]['date']
    df['price_acquisition_approx'] = df['ticker'].apply(lambda x: get_price_of_date_in_df(df=get_df_timeseries_by_ticker(ticker=x), date=ref_date))
    df['return_approx'] = (df['price_last'] - df['price_acquisition_approx']) / df['price_acquisition_approx']*100
    usdkrw = get_df_usdkrw()
    usdkrw = usdkrw[usdkrw.index <= ref_date].iloc[-1]['usdkrw']
    df['acquisition_approx_krw'] = df['price_acquisition_approx'] * df['amount']
    df['acquisition_approx_usd'] = df['acquisition_approx_krw'] / usdkrw
    df['evaluation_krw'] = df['price_last'] * df['amount']
    df['evaluation_usd'] = df['evaluation_krw'] / usdkrw
    return df

def append_market_info_to_history(history):
    history['ticker_bbg'] = history.index.map(lambda x: get_ticker_bbg_of_ticker(x))
    ks = get_ks_market_info()
    df = history.reset_index().merge(ks, how='left', on='ticker_bbg')
    return df

def append_derived_info_to_history(df):
    df['evaluation_krw'] = df['price_last'] * df['amount']
    usdkrw = get_df_usdkrw()
    usdkrw = usdkrw[usdkrw.index <= get_today()].iloc[-1]['usdkrw']
    df['evaluation_usd'] = df['evaluation_krw'] / usdkrw
    return df


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
            date = get_dates_of_order()[self.index]
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
    
