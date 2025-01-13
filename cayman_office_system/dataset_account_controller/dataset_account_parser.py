from .date_utils import map_date_english_str_to_dashed_date
from .dataset_account_loader import open_raw_account_as_df_by_fund_class

def extract_data_info(raw):
    part_info = raw.iloc[:4, :]
    fund_name_raw = part_info.columns[1]
    elms_fund_name = fund_name_raw.split(' ')[:-2]
    fund_name = ' '.join(elms_fund_name)
    fund_id = fund_name_raw.split(' ')[-2]
    fund_currency = fund_name_raw.split(' ')[-1]
    start_date_raw = part_info.iloc[0, 1]
    start_date = map_date_english_str_to_dashed_date(start_date_raw)
    end_date_raw = part_info.iloc[0, 3]
    end_date = map_date_english_str_to_dashed_date(end_date_raw)
    ledger_balance = part_info.iloc[2, 1]
    available_balance = part_info.iloc[3, 1]
    dct = {'fund_name': fund_name, 'fund_id': fund_id, 'fund_currency': fund_currency, 'start_date': start_date, 'end_date': end_date, 'ledger_balance': ledger_balance, 'available_balance': available_balance}
    return dct

def extract_data_footer(raw):
    part_footer = raw.iloc[-2:, :]
    prined_by = part_footer.iloc[0, 1]
    printed_on_raw = part_footer.iloc[1, 1]
    printed_on = map_date_english_str_to_dashed_date(printed_on_raw.split(' ')[0])
    dct = {'printed_by': prined_by, 'printed_on': printed_on}
    return dct

def extract_fund_data(raw):
    data_info = extract_data_info(raw,)
    data_footer = extract_data_footer(raw)
    return {**data_info, **data_footer}

def extract_timeseries_account(raw):
    part_timeseries = raw.iloc[4:-2, :]
    col = part_timeseries.iloc[0, :]
    part_timeseries.columns = col
    part_timeseries.columns.name = None
    part_timeseries = part_timeseries.iloc[1:, :].reset_index(drop=True)
    return part_timeseries

def extract_fund_data_by_fund_class(fund_class, regex=None):
    raw = open_raw_account_as_df_by_fund_class(fund_class, regex=regex)
    fund_data = extract_fund_data(raw)
    return fund_data

map_fund_class_to_fund_data = extract_fund_data_by_fund_class

def get_timeseries_account_by_fund_class(fund_class, regex=None):
    raw = open_raw_account_as_df_by_fund_class(fund_class, regex)
    timeseries_account = extract_timeseries_account(raw)
    return timeseries_account

map_fund_class_to_timeseries = get_timeseries_account_by_fund_class

def get_timeseries_account(regex=None):
    raw = open_raw_account_as_df_by_fund_class('account', regex)
    timeseries_account = extract_timeseries_account(raw)
    return timeseries_account

def compose_filename_dataset_account(fund_data):
    filename = f'dataset-account-{fund_data["fund_name"]}-between{fund_data["start_date"].replace("-","")}-and{fund_data["end_date"].replace("-","")}-save{fund_data["printed_on"].replace("-","")}.csv'
    return filename

map_fund_data_to_filename = compose_filename_dataset_account