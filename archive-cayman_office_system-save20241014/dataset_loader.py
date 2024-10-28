from shining_pebbles import scan_files_including_regex, pick_input_date_in_file_name, open_df_in_file_folder_by_regex
from .dataset_constants import *
import pandas as pd
import os
import re


def get_df_sector_ks():
    sector_ks = open_df_in_file_folder_by_regex(file_folder=file_folder['sector'], regex='ks_name_sector')
    sector_ks = sector_ks.rename(columns=MAPPING_SECTOR)
    return sector_ks

def get_df_usdkrw():
    usdkrw = open_df_in_file_folder_by_regex(file_folder=file_folder['currency'], regex='USDKRW')
    usdkrw = usdkrw.rename(columns={'PX_LAST': 'usdkrw'})
    return usdkrw

def get_usdkrw_of_date(date):
    df = get_df_usdkrw()
    usdkrw = df[df.index <= date].iloc[-1]['usdkrw']
    return usdkrw

def open_excel(file_name, file_folder, engine='openpyxl'):
    file_path = os.path.join(file_folder, file_name)
    return pd.read_excel(file_path, engine=engine)

def open_balance_of_month(month, file_folder=file_folder['balance']):
    file_names = scan_files_including_regex(file_folder=file_folder, regex=f'^.*-{month}.xls$')
    df = open_excel(file_folder=file_folder, file_name=file_names[-1], engine='xlrd')
    return df

def get_data_balance(df):
    df = df.iloc[:4, 1:2]
    keys = ['date_str', 'opening_balance', 'ledger_balance', 'available_balance']
    values = list(df.iloc[: ,0])
    dct = {}
    for k, v in zip(keys, values):
        dct[k] = v
    dd, month, yyyy = dct['date_str'].split('-')
    mapping_month = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    mm = mapping_month[month]
    date = f'{yyyy}-{mm}-{dd}'
    dct['date'] = date
    return dct

def scan_files_order(file_folder=file_folder['order']):
    prefix = FILE_NAME_PREFIX_ORDER
    file_names = scan_files_including_regex(file_folder=file_folder, regex=prefix)
    return file_names

def extract_date_in_file_name(file_name):
    match = re.search(r'\b\d{8}\b', file_name)
    if match:
        return match.group(0)
    return None

def get_order_date_in_file_name(file_name, form='%Y-%m-%d'):
    date = extract_date_in_file_name(file_name)
    if form == '%Y-%m-%d':
        date = f'{date[:4]}-{date[4:6]}-{date[6:]}' if '-' not in date else date
    return date

def get_dates_of_order(file_folder=file_folder['order'], form='%Y-%m-%d'):
    file_names = scan_files_order(file_folder=file_folder)
    dates = [get_order_date_in_file_name(file_name=file_name, form=form) for file_name in file_names]
    dates = sorted(set(dates))
    return dates

def open_df_order_by_index(file_folder=file_folder['order'], index=-1):
    prefix = FILE_NAME_PREFIX_ORDER
    file_name = scan_files_including_regex(file_folder=file_folder, regex=prefix)[index]
    df = open_excel(file_name, file_folder=file_folder)
    date = get_order_date_in_file_name(file_name)
    df['date'] = date
    df = df.set_index('date')
    print(f'- open: {file_name} in {file_folder}')
    return df

def open_df_order_latest(file_folder=file_folder['order']):
    df = open_df_order_by_index(file_folder=file_folder, index=-1)
    return df


def scan_files_including_date_in_file_name_prefix(prefix, date, file_folder):
    date = date.replace('-', '')
    yyyymmdd = date
    regex = f'{prefix}.*{yyyymmdd}'
    file_names = scan_files_including_regex(file_folder=file_folder, regex=regex)
    if len(file_names) == 0:
        mmdd = yyyymmdd[4:]
        regex = f'{prefix}.*{mmdd}'
        file_names = scan_files_including_regex(file_folder=file_folder, regex=regex)
    if len(file_names) == 0:
        file_names = None
    if file_names != None:
        file_names = sorted(file_names)
    return file_names


def open_df_order_by_date(date, file_folder=file_folder['order'], verbose=False):
    prefix = FILE_NAME_PREFIX_ORDER
    file_name = scan_files_including_date_in_file_name_prefix(prefix=prefix, date=date, file_folder=file_folder)[-1]
    df = open_excel(file_name, file_folder=file_folder)
    date = get_order_date_in_file_name(file_name)
    df['date'] = date
    df = df.set_index('date')
    if verbose:
        print(f'- open: {file_name} in {file_folder}')
    return df

def preprocess_df_order(df_order):
    df = df_order.copy()
    cols_to_keep = ['Symbol', 'Amount']
    df = df[cols_to_keep]
    df.columns = [MAPPING_COLUMNS_ORDER[col] for col in df.columns]
    # df = df.set_index('ticker')
    return df

def get_df_order_latest(file_folder=file_folder['order']):
    df_order = open_df_order_latest(file_folder=file_folder)
    df_order = preprocess_df_order(df_order)
    return df_order

def get_df_order_by_date(date, file_folder=file_folder['order']):
    df_order = open_df_order_by_date(date, file_folder=file_folder)
    df_order = preprocess_df_order(df_order)
    return df_order

def merge_df_orders(dfs_order):
    df = pd.concat(dfs_order, axis=0)
    return df


def open_df_holding_latest(file_folder=file_folder['holding']):
    prefix = FILE_NAME_PREFIX_HOLDING
    file_name = scan_files_including_regex(file_folder=file_folder, regex=prefix)[-1]
    date = pick_input_date_in_file_name(file_name)
    date = f'{date[:4]}-{date[4:6]}-{date[6:]}'
    df = open_df_in_file_folder_by_regex(file_folder=file_folder, regex=prefix)
    df.columns.name = date
    print(f'- open: {file_name} in {file_folder}')
    return df

def merge_order_to_holding(df_holding, df_order):
    df = df_holding.merge(df_order, how='outer', left_index=True, right_index=True, suffixes=('_asof', '_order'))
    df = df.fillna(0)
    df['num_tobe'] = df['num_asof'] + df['num_order']
    for col in df.columns:
        df[col] = df[col].astype(int)
    return df

def generate_df_holding_from_df_merge(df_merge):
    df = df_merge.copy()
    df = df[['num_tobe']]
    return df
