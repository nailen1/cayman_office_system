from .dataset_account_utils import get_df_account_credit_and_debit_of_fund_class
from .dataset_operation_parser import get_sum_of_revenues_and_expenses_at_month_end
from .valid_account_logs_consts import VALID_ACCOUNT_LOGS, VALID_DATES_FOR_INCOME_LOSS_DATA
from .flow_utils import get_timeseries_flow
from shining_pebbles import get_today, get_date_range
from ..dataset_loader import get_df_usdkrw, get_usdkrw_of_date
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

def append_income_loss_to_timeseries(ts):
    if 'revenue/expense' not in ts.columns:
        ts['revenue/expense'] = 0.0

    for month_end in VALID_DATES_FOR_INCOME_LOSS_DATA:
        net, date = get_sum_of_revenues_and_expenses_at_month_end(end_date=month_end)
        print(f'net: {net}, date: {date}')
        if date not in ts.index:
            ts.loc[date] = {'revenue/expense': net}
        else:
            ts.loc[date, 'revenue/expense'] = net
    ts = ts.fillna(0)
    # ts['net_flow'] = ts.filter(like='credit').sum(axis=1) - ts.filter(like='debit').sum(axis=1) + ts['net revenue/expense']
    return ts

def get_timeseries_account():
    ts = get_timeseries_flow()
    ts = append_income_loss_to_timeseries(ts)
    ts = ts.sort_index(ascending=True)
    return ts

def get_timeseries_master():
    account = get_timeseries_account()
    account['netflow'] = account['inflow'] - account['outflow']
    cols_to_keep = ['netflow', 'revenue/expense']
    master = account[cols_to_keep].rename(columns={'netflow': 'flow: usd', 'revenue/expense': 'income: usd'})
    all_dates = get_date_range(master.index[0], get_today())
    master = master.reindex(all_dates)
    usdkrw = get_df_usdkrw()
    master = master.merge(usdkrw, left_index=True, right_index=True, how='left')
    master.loc[(master.index[0], 'usdkrw')] = get_usdkrw_of_date(master.index[0])
    master['usdkrw'] = master['usdkrw'].ffill()
    # master['netflow: krw'] = master['netflow: usd'] * master['usdkrw']
    # master['net income: krw'] = master['net income: usd'] * master['usdkrw']
    master = master.fillna(0)
    return master

