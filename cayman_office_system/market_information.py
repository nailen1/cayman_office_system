from  shining_pebbles import get_today
from .dataset_loader import get_df_sector_ks, get_df_price_usdlevetf, get_df_cap_usdlevetf
from .birdeye_connector import get_df_prices_of_ks_stock
from .finance_utils import get_ticker_from_ticker_bbg, get_ticker_bbg_of_ticker


ROW_INFO_FOR_USD_LEVERAGED_EFT = {
    'ticker_bbg': '261250 KS Equity',
    'name': 'SAMSUNG KODEX USD FTRS LEV',
    'name_kr': 'KODEX 미국달러선물레버리지',
    'market_index': 'KOSPI Index',
    'sector': 'ETF',
    'cap(/1e8)': get_df_cap_usdlevetf().iloc[-1]['cap(/1e6)']*1e6/1e8,
    'price_last': get_df_price_usdlevetf().iloc[-1]['price_last'],
}


class MarketInfo:
    def __init__(self, name='ks'):
        self.name = name
        self.prices = self.get_prices()
        self.sector = self.get_sector()
        self.info = self.get_info()
    
    def get_prices(self):
        mapping = {
            'ks': get_df_prices_of_ks_stock
        }
        prices = mapping[self.name]()
        self.prices = prices
        return prices
    
    def get_sector(self):
        mapping = {
            'ks': get_df_sector_ks
        }
        sector = mapping[self.name]()
        self.sector = sector
        return sector
    
    def get_info(self, include_usd_leveraged_etf=True):
        info = self.sector.merge(self.prices, how='outer', left_index=True, right_index=True)
        cols_ordered = ['name_kr', 'name', 'market_index', 'sector', 'cap(/1e8)', 'price_last']
        info = info[cols_ordered]
        if include_usd_leveraged_etf:
            info.loc[ROW_INFO_FOR_USD_LEVERAGED_EFT['ticker_bbg']] = ROW_INFO_FOR_USD_LEVERAGED_EFT
        self.info = info
        return info

    def get_mapping_name(self):
        self.mapping_name = self.sector['name'].to_dict()
        return self.mapping_name
    
    def get_mapping_sector(self):
        self.mapping_sector = self.sector['sector'].to_dict()
        return self.mapping_sector
    

def get_mapping_ks_name():
    name_sector = get_df_sector_ks()
    return name_sector['name'].to_dict()

def get_mapping_ks_sector():
    name_sector = get_df_sector_ks()
    return name_sector['sector'].to_dict()

def get_ks_market_info():
    return MarketInfo(name='ks').info

def append_market_info_to_df(df):
    df['ticker_bbg'] = df['ticker'].map(lambda x: get_ticker_bbg_of_ticker(x))
    ks = get_ks_market_info().reset_index()
    df = df.merge(ks, how='left', on='ticker_bbg')
    return df

def get_ks_equity_info(tickers):
    tickers_bbg = [get_ticker_bbg_of_ticker(ticker) for ticker in tickers]
    ks = get_ks_market_info()
    df = ks[ks.index.isin(tickers_bbg)]
    df.columns.name = get_today()
    return df

def get_list_of_ks_stocks():
    stock_info = get_ks_market_info()
    stock_info['ticker'] = stock_info.index.map(get_ticker_from_ticker_bbg)
    stock_info = stock_info.fillna('-')
    return stock_info

