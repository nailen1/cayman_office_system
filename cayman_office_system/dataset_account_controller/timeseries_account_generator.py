from .dataset_account_utils import get_df_account_credit_and_debit_of_fund_class
from .dataset_operation_parser import get_net_cashflow_at_month_end
from .valid_account_logs_consts import VALID_ACCOUNT_LOGS, VALID_DATES_FOR_NET_INCOME_LOSS_DATA
import pandas as pd

def get_merged_df_account_credit_and_debit():
    credit_and_debit1 = get_df_account_credit_and_debit_of_fund_class(fund_class='feeder1')
    credit_and_debit2 = get_df_account_credit_and_debit_of_fund_class(fund_class='feeder2')
    credit_and_debit = credit_and_debit1.merge(credit_and_debit2, left_index=True, right_index=True, how='outer').reset_index()
    return credit_and_debit

def filter_df_by_valid_logs_of_credit_and_debit(df, valid_logs=VALID_ACCOUNT_LOGS):
    valid_rows = []
    for column, dates in valid_logs.items():
        for date in dates:
            mask = (df['date'] == date) & (pd.notnull(df[column]))
            if mask.any():
                valid_rows.extend(df[mask].index.tolist())
    filtered_df = df.loc[valid_rows]
    return filtered_df

def preprocess_df_account_credit_and_debit_for_timeseries(df):
    df = df.fillna(0)
    # df['cash'] = df.drop(columns=['date']).sum(axis=1)
    df = df.groupby('date').sum()
    return df

def get_timeseries_account_credit_and_debit():
    return preprocess_df_account_credit_and_debit_for_timeseries(filter_df_by_valid_logs_of_credit_and_debit(get_merged_df_account_credit_and_debit()))

def append_net_income_loss_to_timeseries(ts):
    if 'net income/loss' not in ts.columns:
        ts['net income/loss'] = 0.0

    for month_end in VALID_DATES_FOR_NET_INCOME_LOSS_DATA:
        net, date = get_net_cashflow_at_month_end(end_date=month_end)
        if date not in ts.index:
            ts.loc[date] = {'net income/loss': net}
        else:
            ts.loc[date, 'net income/loss'] = net
    ts = ts.fillna(0)
    ts['net_flow'] = ts.filter(like='credit').sum(axis=1) - ts.filter(like='debit').sum(axis=1) + ts['net income/loss']
    return ts

def get_timeseries_account():
    ts = get_timeseries_account_credit_and_debit()
    ts = append_net_income_loss_to_timeseries(ts)
    return ts


