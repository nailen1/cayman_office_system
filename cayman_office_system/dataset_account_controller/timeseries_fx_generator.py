from .dataset_account_utils import get_df_account_sellbuy_by_fund_class
from .number_utils import transform_number_like_value_to_float

def extract_amount_from_transaction_description(description, side):
    if side not in ['RVP', 'DVP']:
        raise ValueError("side must be either 'RVP' or 'DVP'.")
    if side in description:
        amount = description.split('@')[0].replace(' ','').split('KRW')[-1]
    else:
        amount = None
    return amount

def extract_fx_from_transaction_description(description):
    fx = description.split('@')[-1].replace('RATE', '')
    return fx

def get_preprocessed_df_account_fx(fund_class):
    df = get_df_account_sellbuy_by_fund_class(fund_class)
    for side in ['RVP', 'DVP']:
        df[side] = df['Transaction Description 2'].apply(lambda x: extract_amount_from_transaction_description(x, side))
    df['FX'] = df['Transaction Description 2'].apply(lambda x: extract_fx_from_transaction_description(x))
    cols_preprocessed = ['RVP', 'DVP', 'FX']
    for col in cols_preprocessed:
        df[col] = df[col].apply(transform_number_like_value_to_float)
    df = df[cols_preprocessed].fillna(0)
    df = df.reset_index()
    return df

def get_df_account_FX(fund_class='master'):
    df = get_preprocessed_df_account_fx(fund_class)
    return df

def get_timeseries_account_fx(fund_class='master'):
    df = get_df_account_FX(fund_class)
    df = df.drop_duplicates(subset=['date'])
    cols_to_keep = ['date', 'FX']
    df = df[cols_to_keep].set_index('date')
    return df