from .dataset_constants import file_folder, FILE_NAME_PREFIX_ORDER
from shining_pebbles import scan_files_including_regex
from datetime import datetime
import re

def get_date_from_file_name(file_name, form):
    match = re.search(r'\d{8}', file_name)
    if not match:
        return None
    
    date_str = match.group()
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    
    return date_obj.strftime(form)


def get_dates_of_documents_in_file_folder(file_folder, regex, form='%Y-%m-%d'):
    file_names = scan_files_including_regex(file_folder=file_folder, regex=regex)
    dates = [get_date_from_file_name(file_name=file_name, form=form) for file_name in file_names]
    return dates