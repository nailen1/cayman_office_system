from  shining_pebbles import get_today
from .pseudo_database import get_df_sector_ks, get_df_price_usdlevetf, get_df_cap_usdlevetf
from .birdeye_connector import get_df_prices_of_ks_stock


ROW_INFO_FOR_USD_LEVERAGED_EFT = {
    'ticker_bbg': '261250 KS Equity',
    'name': 'SAMSUNG KODEX USD FTRS LEV',
    'name_kr': 'KODEX 미국달러선물레버리지',
    'market_index': 'KOSPI Index',
    'sector': 'ETF',
    'cap(/1e8)': get_df_cap_usdlevetf().iloc[-1]['cap(/1e6)']*1e6/1e8,
    'price_last': get_df_price_usdlevetf().iloc[-1]['price_last'],
}


class MarketInformation:
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
    