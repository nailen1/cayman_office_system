from .dataset_constants import file_folder, FILE_NAME_PREFIX_ORDER
from .dataset_loader import open_df_order_by_date
from .dataset_utils import get_dates_of_documents_in_file_folder
import pandas as pd

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
