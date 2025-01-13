from .dataset_account_loader import open_dataset_account
from .date_utils import map_date_english_str_to_dashed_date

def get_df_account_by_fund_class(fund_class):
    df = open_dataset_account(fund_class=fund_class)
    df['date'] = df['Date'].map(map_date_english_str_to_dashed_date)
    df = df.set_index('date').drop(columns=['Date', 'Value Date', 'Transaction Description 1'])
    return df

def get_df_account_sellbuy_by_fund_class(fund_class):
    df = get_df_account_by_fund_class(fund_class)
    df = df[df['Transaction Description 2'].str.contains('RVP|DVP')]
    return df

def get_df_account_remainder_by_fund_class(fund_class):
    df = get_df_account_by_fund_class(fund_class)
    df = df[~df['Transaction Description 2'].str.contains('RVP|DVP')]
    return df

def get_timeseries_balance_in_master():
    cols_to_keep = ['Debit', 'Credit']
    ts_balance = get_df_account_remainder_by_fund_class(fund_class='master')[cols_to_keep].fillna(0)
    ts_balance = ts_balance.reset_index()
    ts_balance = ts_balance.groupby('date').sum()
    ts_balance['balance_master: usd'] = (ts_balance['Credit'] - ts_balance['Debit']).cumsum()
    return ts_balance

def get_timeseries_credit_and_debit_in_master():
    ts_balance = get_df_account_remainder_by_fund_class(fund_class='master')
    ts_balance = ts_balance.groupby('date').sum()
    ts_balance = ts_balance[['Credit', 'Debit']].rename(columns={'Debit': 'debit_master: usd', 'Credit': 'credit_master: usd'})
    return ts_balance

def get_df_account_credit_and_debit_of_fund_class(fund_class):
    account = get_df_account_remainder_by_fund_class(fund_class=fund_class)
    cols_to_keep = ['Credit', 'Debit']
    cashflow = account[cols_to_keep]
    cashflow.columns = [f'credit: {fund_class}', f'debit: {fund_class}']
    return cashflow
