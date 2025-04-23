from shining_pebbles import open_df_in_file_folder_by_regex
from cayman_office_system.path_director import file_folder
from .sector_hotfix_consts import HOTFIX_DATA_SECTOR
from .pseudo_database_consts import MAPPING_SECTOR


def get_df_sector_ks():
    sector_ks = open_df_in_file_folder_by_regex(file_folder=file_folder['market'], regex='ks_market')
    for hotfix_datum in HOTFIX_DATA_SECTOR:
        sector_ks.loc[hotfix_datum['ticker_bbg']] = hotfix_datum
    sector_ks = sector_ks.rename(columns=MAPPING_SECTOR)
    return sector_ks

def get_df_usdkrw():
    usdkrw = open_df_in_file_folder_by_regex(file_folder=file_folder['currency'], regex='USDKRW KRWT Curncy')
    usdkrw = usdkrw.rename(columns={'PX_LAST': 'usdkrw'})
    return usdkrw

def get_usdkrw_of_date(date):
    df = get_df_usdkrw()
    usdkrw = df[df.index <= date].iloc[-1]['usdkrw']
    return usdkrw

def get_df_price_usdlevetf():
    fld = 'PX_LAST'
    df = open_df_in_file_folder_by_regex(file_folder=file_folder['bbg'], regex=f'261250 KS Equity-{fld}')
    df = df.rename(columns={f'{fld}': 'price_last'})
    return df

def get_df_cap_usdlevetf():
    fld = 'CUR_MKT_CAP'
    df = open_df_in_file_folder_by_regex(file_folder=file_folder['bbg'], regex=f'261250 KS Equity-{fld}')
    df = df.rename(columns={f'{fld}': 'cap(/1e6)'})
    return df
