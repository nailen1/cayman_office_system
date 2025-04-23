import pandas as pd
from shining_pebbles import get_today, scan_files_including_regex
from cayman_office_system.path_director import file_folder
from cayman_office_system.cos_dataset_loader import open_df_order_by_date, get_dates_of_documents_in_file_folder, FILE_NAME_PREFIX_ORDER
from cayman_office_system.finance_utils import get_ticker_bbg_of_ticker
from cayman_office_system.market_database import (
    get_ks_market_info, 
    get_df_timeseries_by_ticker, 
    get_price_of_date_in_df,
    get_df_usdkrw
)

def get_dates_of_order_in_file_folder(file_folder=file_folder['order'], form='%Y-%m-%d'):
    dates = get_dates_of_documents_in_file_folder(file_folder=file_folder, regex=FILE_NAME_PREFIX_ORDER, form=form)
    return dates

def merge_orders():
    dates = get_dates_of_order_in_file_folder()
    dfs = []
    for date in dates:
        try:
            df = open_df_order_by_date(date=date)
            dfs.append(df)
        except Exception as e:
            continue
    df = pd.concat(dfs)
    return df

def filter_columns_in_orders(df_orders):
    df_rationales = df_orders[['Symbol', 'Side', 'Rationale']]
    df_rationales = df_rationales.rename(columns={'Symbol': 'ticker', 'Rationale': 'rationale', 'Side': 'type'})
    return df_rationales

def map_filtered_to_rationale(df_filtered, ticker, type):
    df_rationale = df_filtered[(df_filtered['ticker'] == ticker)&(df_filtered['type'] == type[:1])]
    df_rationale = df_rationale.drop_duplicates(subset='rationale')
    df_rationale = df_rationale.sort_index(ascending=False)
    return df_rationale

def get_df_rationale(ticker, type):
    df_orders = merge_orders()
    df_rationales = filter_columns_in_orders(df_orders)
    df_rationale = map_filtered_to_rationale(df_rationales, ticker, type)
    return df_rationale

def append_market_info_to_sheet(sheet):
    sheet['ticker_bbg'] = sheet['ticker'].map(lambda x: get_ticker_bbg_of_ticker(x))
    ks = get_ks_market_info()
    df = sheet.reset_index().merge(ks, how='left', on='ticker_bbg')
    return df

def append_derived_info_to_sheet(df):
    ref_date = df.iloc[0]['date']
    df['price_acquisition_approx'] = df['ticker'].apply(lambda x: get_price_of_date_in_df(df=get_df_timeseries_by_ticker(ticker=x), date=ref_date))
    df['return_approx'] = (df['price_last'] - df['price_acquisition_approx']) / df['price_acquisition_approx']*100
    usdkrw = get_df_usdkrw()
    usdkrw = usdkrw[usdkrw.index <= ref_date].iloc[-1]['usdkrw']
    df['acquisition_approx_krw'] = df['price_acquisition_approx'] * df['amount']
    df['acquisition_approx_usd'] = df['acquisition_approx_krw'] / usdkrw
    df['evaluation_krw'] = df['price_last'] * df['amount']
    df['evaluation_usd'] = df['evaluation_krw'] / usdkrw
    return df

def append_market_info_to_history(history):
    history['ticker_bbg'] = history.index.map(lambda x: get_ticker_bbg_of_ticker(x))
    ks = get_ks_market_info()
    df = history.reset_index().merge(ks, how='left', on='ticker_bbg')
    return df

def append_derived_info_to_history(df):
    df['evaluation_krw'] = df['price_last'] * df['amount']
    usdkrw = get_df_usdkrw()
    usdkrw = usdkrw[usdkrw.index <= get_today()].iloc[-1]['usdkrw']
    df['evaluation_usd'] = df['evaluation_krw'] / usdkrw
    return df

