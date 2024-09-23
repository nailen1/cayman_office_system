
import re
from datetime import datetime
from shining_pebbles import scan_files_including_regex
from .dataset_constants import file_folder
from .dataset_loader import open_excel


KEYS_TRANSACTION = ['Type',
 'ISIN Code / Abbr. Code',
 'Security Description',
 'Total No. of Shares',
 'Average Price',
 'Considerations',
 'Commission',
 'Sales Tax',
 'Capital Gains Tax',
 'Net Amount']

KEY_SELLBUY = 'No. of Shares / Price'

# FILE_NAME_PREFIX_TRADE = 'LKEF Trade'
FILE_NAME_PREFIX_TRADE = 'Samsung Securities Co., Ltd.'

def get_date_from_file_name(file_name, form):
    match = re.search(r'\d{8}', file_name)
    if not match:
        return None
    
    date_str = match.group()
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    
    return date_obj.strftime(form)

def get_dates_of_trades_in_file_folder(file_folder=file_folder['trade'], form='%Y-%m-%d'):
    file_names = scan_files_including_regex(file_folder=file_folder, regex=FILE_NAME_PREFIX_TRADE)
    dates = [get_date_from_file_name(file_name=file_name, form=form) for file_name in file_names]
    return dates

def open_df_trade_by_date(date, file_folder=file_folder['trade'], verbose=False):
    date = date.replace('-', '')
    regex = FILE_NAME_PREFIX_TRADE+f'.*{date}'
    file_name = scan_files_including_regex(file_folder=file_folder, regex=regex)[-1]
    df = open_excel(file_folder=file_folder, file_name=file_name, engine='xlrd')
    if verbose:
        print('File Name: ', file_name)
    return df

def open_df_trade_by_index(index, file_folder=file_folder['trade'], verbose=False):
    regex = FILE_NAME_PREFIX_TRADE
    file_name = scan_files_including_regex(file_folder=file_folder, regex=regex)[index]
    df = open_excel(file_folder=file_folder, file_name=file_name, engine='xlrd')
    if verbose:
        print('File Name: ', file_name)
    return df

def get_keys_from_df_trade(df):
    keys = sorted(list(df['Unnamed: 0'].dropna()))
    return keys

def get_data_indicies_of_keys(keys, df):
    dct_indices = {}
    for key in keys:
        indices_of_key = list(df[df['Unnamed: 0']==key].index)
        dct_indices[key] = indices_of_key
    return dct_indices
    
def get_pair_index_of_info(dct_indices):
    index_i = 0
    index_f = dct_indices['Type'][0]-1
    return (index_i, index_f)

def get_pairs_index_of_transaction(dct_indices):
    indices_i = dct_indices['Type']
    indices_f = dct_indices['Net Amount']
    pairs_index = [(index_i, index_f+1) for index_i, index_f in zip(indices_i, indices_f)]
    return pairs_index

def get_df_raw_info(df, pair_info):
    index_i, index_f = pair_info
    df_info = df.iloc[index_i:index_f]
    return df_info

def get_df_raw_transaction(df, pair_raw_transaction):
    index_i, index_f = pair_raw_transaction
    df_raw_transaction = df.iloc[index_i:index_f]
    return df_raw_transaction

def get_df_transaction_by_index(df, index, pairs_transaction):
    pair_transaction = pairs_transaction[index]
    df_transaction = get_df_raw_transaction(df, pair_transaction)
    return df_transaction

# def get_dfs_transaction(df, pairs_trade):
#     dfs = {}
#     for index in range(len(pairs_trade)):
#         df_trade = get_df_transaction_by_index(df, index, pairs_trade)
#         dfs[index] = df_trade
#     return dfs

def get_dfs_transaction(df, pairs_transaction):
    dfs = []
    for i, pair in enumerate(pairs_transaction):
        df_transaction = get_df_transaction_by_index(df, i, pair)
        dfs.append(df_transaction)
    return dfs

def get_row_in_transaction(transaction, key):
    row = transaction[transaction['Unnamed: 0']==key]
    return row

def get_values_of_key_in_transaction(transaction, key):
    row = get_row_in_transaction(transaction, key)
    srs = row.dropna(axis=1).iloc[0]
    values = list(srs[1:])
    return values

def get_data_in_transaction(transaction, keys):
    dct = {}
    for key in keys:
        values = get_values_of_key_in_transaction(transaction, key)
        dct[key] = values
    return dct

def get_df_sellbuy(transaction):
    df = transaction[~transaction['Unnamed: 6'].isna()].dropna(axis=1)
    df.columns = ['num_shares', 'currency', 'price_executed']
    return df

def get_ticker_in_transaction(data):
    isin_code, abbr_code = data['ISIN Code / Abbr. Code'][-1].split('/')
    isin_code, abbr_code = isin_code.strip(), abbr_code.strip()
    ticker = f'{isin_code[3:-3]} KS'
    return ticker

def get_type_in_transaction(data):
    return data['Type'][-1]

def get_isin_and_abbr_code_in_transaction(data):
    isin_code, abbr_code = data['ISIN Code / Abbr. Code'][-1].split('/')
    isin_code, abbr_code = isin_code.strip(), abbr_code.strip()
    return isin_code, abbr_code

def get_isin_code_in_transaction(data):
    return get_isin_and_abbr_code_in_transaction(data)[0]

def get_abbr_code_in_transaction(data):
    return get_isin_and_abbr_code_in_transaction(data)[1]

def get_ticker_in_transaction(data):
    isin_code = get_isin_code_in_transaction(data)
    ticker = f'{isin_code[3:-3]} KS'
    return ticker

def get_name_in_transaction(data):
    return data['Security Description'][-1]

def get_consideration_in_transaction(data):
    return data['Considerations'][-1]

def get_commission_in_transaction(data):
    return data['Commission'][-1]

def get_sales_tax_in_transaction(data):
    return data['Sales Tax'][-1]

def get_capital_gains_tax_in_transaction(data):
    return data['Capital Gains Tax'][-1]

def get_net_amount_in_transaction(data):
    return data['Net Amount'][-1]

def get_total_no_of_shares_in_transaction(data):
    return data['Total No. of Shares'][-1]

def get_average_price_in_transaction(data):
    return data['Average Price'][-1]
    