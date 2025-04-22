from .dataset_account_utils import get_df_account_remainder_by_fund_class
from .flow_consts import INFLOW_DESCRIPTIONS_OF_REMARKS_AND_DATES
import pandas as pd

def append_remark_on_df_account_by_description(df_account, description, remark):
    df_account.loc[df_account['Transaction Description 2']==description, 'remark'] = remark
    return df_account

def append_remarks_on_df_account_by_descriptions(df_account, descriptions_and_remarks):
    for description, remark in descriptions_and_remarks.items():
        df_account = append_remark_on_df_account_by_description(df_account, description, remark)
    return df_account

def get_df_flow_by_fund_class(fund_class, descriptions_and_remarks):
    df = get_df_account_remainder_by_fund_class(fund_class)
    df = append_remarks_on_df_account_by_descriptions(df, descriptions_and_remarks=descriptions_and_remarks)
    return df

def get_df_flow():
    feeder1 = get_df_account_remainder_by_fund_class(fund_class='feeder1')
    feeder2 = get_df_account_remainder_by_fund_class(fund_class='feeder2')

    df = pd.concat([feeder1, feeder2], axis=0)
    for description, (remark, date) in INFLOW_DESCRIPTIONS_OF_REMARKS_AND_DATES.items():
        df.loc[df['Transaction Description 2'] == description, 'remark'] = remark
        df.loc[df['Transaction Description 2'] == description, 'date'] = date

    cols_to_keep = ['date', 'Transaction Description 2', 'Credit', 'Debit', 'remark']
    df = df[cols_to_keep]
    df = df[df['remark'].notnull()].set_index('date').fillna(0)
    # df['flow'] = df['Credit'] - df['Debit']
    return df

def get_timeseries_flow():
    df = get_df_flow().reset_index()
    df = df.groupby('date').sum()
    cols_to_keep = ['Credit', 'Debit']
    df = df[cols_to_keep]
    df = df.rename(columns={'Credit': 'inflow', 'Debit': 'outflow'})
    return df