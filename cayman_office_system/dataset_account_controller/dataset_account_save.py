from ..path_director import file_folder
from ..dataset_loader import open_excel
from .dataset_account_parser import extract_fund_data, extract_timeseries_account, compose_filename_dataset_account
from canonical_transformer import map_df_to_csv
from shining_pebbles import scan_files_including_regex

def save_dataset_account_from_raw_account_by_fund_class(fund_class, file_name):
    raw = open_excel(file_folder=file_folder[fund_class], file_name=file_name, engine='xlrd')
    fundinfo = extract_fund_data(raw)
    file_name_save = compose_filename_dataset_account(fundinfo)
    df = extract_timeseries_account(raw)
    map_df_to_csv(df, file_folder=file_folder['account'], file_name=file_name_save)
    print(f'|- save: {file_name_save}')
    return df

map_fund_class_to_csv = save_dataset_account_from_raw_account_by_fund_class

def save_datasets_account_by_fund_class(fund_class, regex=None, range=None):
    regex = '.*.xls' if regex is None else regex
    filenames = scan_files_including_regex(file_folder[fund_class], regex=regex)
    if range is not None:
        filenames = filenames[range[0]:range[1]]
    for file_name in filenames:
        print(file_name)
        save_dataset_account_from_raw_account_by_fund_class(fund_class, file_name)
    print(f'|- saved datasets account')
    return None
    
map_fund_class_to_csvs = save_datasets_account_by_fund_class

def save_dataset_account_from_raw_account(file_name):
    raw = open_excel(file_folder=file_folder['account-raw'], file_name=file_name, engine='xlrd')
    fundinfo = extract_fund_data(raw)
    file_name_save = compose_filename_dataset_account(fundinfo)
    df = extract_timeseries_account(raw)
    map_df_to_csv(df, file_folder=file_folder['account'], file_name=file_name_save)
    print(f'|- save: {file_name_save}')
    return df

map_account_raw_to_account_csv = save_dataset_account_from_raw_account

def save_datasets_account_from_raws_account(range=None):
    filenames = scan_files_including_regex(file_folder['account-raw'], regex='.*.xls')
    if range is not None:
        filenames = filenames[range[0]:range[1]]
    for file_name in filenames:
        save_dataset_account_from_raw_account(file_name)
    return None

map_account_raws_to_account_csvs = save_datasets_account_from_raws_account
