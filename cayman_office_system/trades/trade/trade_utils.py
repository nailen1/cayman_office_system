
from shining_pebbles import scan_files_including_regex, load_xls_in_file_folder_by_regex
from cayman_office_system.path_director import file_folder
from cayman_office_system.cos_dataset_loader import get_dates_of_documents_in_file_folder, FILE_NAME_PREFIX_TRADE
from cayman_office_system.market_database import get_df_timeseries_by_ticker

def get_df_timeseries_of_a_ticker_in_a_trade(trade_data):
    date = trade_data['date']
    ticker = trade_data['ticker']
    # num_shares = trade_data['num_shares']
    delta_shares = trade_data['delta_shares']
    df = get_df_timeseries_by_ticker(ticker=ticker)
    df = df[df.index >= date]
    df['delta_shares'] = delta_shares
    df[f'{ticker}: ({date}, {delta_shares})'] = df['price_last'] * df['delta_shares']
    return df

def get_dates_of_trades_in_file_folder(file_folder=file_folder['trade'], form='%Y-%m-%d'):
    dates = get_dates_of_documents_in_file_folder(file_folder=file_folder, regex=FILE_NAME_PREFIX_TRADE, form=form)
    return dates

def open_df_trade_by_date(date, file_folder=file_folder['trade'], verbose=False):
    date = date.replace('-', '')
    regex = FILE_NAME_PREFIX_TRADE+f'.*{date}'
    file_name = scan_files_including_regex(file_folder=file_folder, regex=regex)[-1]
    df = load_xls_in_file_folder_by_regex(file_folder=file_folder, regex=regex)
    if verbose:
        print('File Name: ', file_name)
    return df

def open_df_trade_by_index(index, file_folder=file_folder['trade'], verbose=False):
    regex = FILE_NAME_PREFIX_TRADE
    file_name = scan_files_including_regex(file_folder=file_folder, regex=regex)[index]
    df = load_xls_in_file_folder_by_regex(file_folder=file_folder, regex=regex)
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

def get_dfs_transaction(df, pairs_transaction):
    dfs = []
    for i, pair in enumerate(pairs_transaction):
        df_transaction = get_df_transaction_by_index(df, i, pair)
        dfs.append(df_transaction)
    return dfs
