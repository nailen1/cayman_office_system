
def get_df_sellbuy(transaction):
    df = transaction[~transaction['Unnamed: 6'].isna()].dropna(axis=1)
    # (2025-04-16) single-row sell trade exception, BRIDGE BIOTHERAPEUTICS
    COLS_TO_KEEP_HOTFIX = ['Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6']
    df = df[COLS_TO_KEEP_HOTFIX]
    df.columns = ['num_shares', 'currency', 'price_executed']
    return df

def get_data_in_transaction(transaction, keys):
    dct = {}
    for key in keys:
        values = get_values_of_key_in_transaction(transaction, key)
        dct[key] = values
    return dct

def get_values_of_key_in_transaction(transaction, key):
    row = get_row_in_transaction(transaction, key)
    srs = row.dropna(axis=1).iloc[0]
    values = list(srs[1:])
    return values

def get_row_in_transaction(transaction, key):
    row = transaction[transaction['Unnamed: 0']==key]
    return row

def get_type_in_transaction(data):
    return data['Type'][-1]

def get_isin_and_abbr_code_in_transaction(data):
    isin_code, abbr_code = data['ISIN Code / Abbr. Code'][-1].split('/')
    isin_code, abbr_code = isin_code.strip(), abbr_code.strip()
    return isin_code, abbr_code

def get_isin_code_in_transaction(data):
    return get_isin_and_abbr_code_in_transaction(data)[0]

def get_abbr_code_in_transaction(data):
    return get_isin_and_abbr_code_in_transaction(data)[1]

def get_ticker_in_transaction(data):
    isin_code = get_isin_code_in_transaction(data)
    ticker = f'{isin_code[3:-3]} KS'
    return ticker

def get_name_in_transaction(data):
    return data['Security Description'][-1]

def get_consideration_in_transaction(data):
    return data['Considerations'][-1]

def get_commission_in_transaction(data):
    return data['Commission'][-1]

def get_sales_tax_in_transaction(data):
    return data['Sales Tax'][-1]

def get_capital_gains_tax_in_transaction(data):
    return data['Capital Gains Tax'][-1]

def get_net_amount_in_transaction(data):
    return data['Net Amount'][-1]

def get_total_no_of_shares_in_transaction(data):
    return data['Total No. of Shares'][-1]

def get_average_price_in_transaction(data):
    return data['Average Price'][-1]
    