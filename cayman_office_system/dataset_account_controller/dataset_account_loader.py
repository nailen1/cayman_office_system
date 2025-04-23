from shining_pebbles import scan_files_including_regex, open_df_in_file_folder_by_regex
from cayman_office_system.path_director import file_folder as file_folder_account
from cayman_office_system.cos_dataset_loader import open_excel
from .dataset_account_consts import MAPPING_LKEF, MAPPING_DATE_GENESIS
from .account_hotfix_consts import HOTFIX_DATA_ACCOUNT

map_regex_to_df = open_df_in_file_folder_by_regex

def open_raw_account_as_df_by_fund_class(fund_class, regex=None):
    regex = '.*.xls' if regex is None else regex
    file_folder = file_folder_account[fund_class]
    file_names = scan_files_including_regex(file_folder=file_folder, regex=regex)
    file_name = file_names[-1]
    print(f'load: {file_name}')
    df = open_excel(file_folder=file_folder, file_name=file_name, engine='xlrd')
    return df

def open_raw_account_as_df(regex=None):
    regex = '.*.xls' if regex is None else regex
    file_folder = file_folder_account['account-raw']
    file_names = scan_files_including_regex(file_folder=file_folder, regex=regex)
    file_name = file_names[-1]
    print(f'load: {file_name}')
    df = open_excel(file_folder=file_folder, file_name=file_name, engine='xlrd')
    return df

def open_dataset_account(fund_class, start_date=None, end_date=None, hotfix_data=HOTFIX_DATA_ACCOUNT):
    if start_date is None and end_date is None:
        regex = f'dataset-account-{MAPPING_LKEF[fund_class]}-between{MAPPING_DATE_GENESIS[fund_class].replace("-","")}-.*.csv'
    elif start_date is not None and end_date is None:
        regex = f'dataset-account-{MAPPING_LKEF[fund_class]}-between{start_date}-.*.csv'
    elif start_date is None and end_date is not None:
        regex = f'dataset-account-{MAPPING_LKEF[fund_class]}-between.*-and{end_date}-.*.csv'
    else:
        regex = f'dataset-account-{MAPPING_LKEF[fund_class]}-between{start_date}-and{end_date}-.*.csv'
    df = map_regex_to_df(file_folder=file_folder_account['account'], regex=regex)
    for hotfix_datum in hotfix_data:
        df = inject_hotfix_exceptions(df, fund_class, hotfix_datum)
    return df

def inject_hotfix_exceptions(df, fund_class, hotfix_datum):
    if hotfix_datum['fund_class'] == fund_class:
        df.loc[
            (
                df['Date'] == hotfix_datum['Date']) & (df['Transaction Description 2'] == hotfix_datum['Transaction Description 2']
            ),
            'Transaction Description 2'] = hotfix_datum['hotfix']
    return df

map_dataset_account_inputs_to_dataset_account = open_dataset_account
