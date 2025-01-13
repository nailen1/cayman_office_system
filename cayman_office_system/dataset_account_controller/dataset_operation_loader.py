from shining_pebbles import open_df_in_file_folder_by_regex
from ..path_director import file_folder

def open_df_operation_by_end_date(end_date):
    end_date = end_date.replace('-', '')
    regex = f'dataset-operation-.*01-and{end_date}-.*'
    df = open_df_in_file_folder_by_regex(file_folder=file_folder['operation'], regex=regex)
    return df