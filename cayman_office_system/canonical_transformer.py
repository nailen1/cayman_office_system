import pandas as pd
import os
import json


def map_df_to_data(df, capitalize=False):
    df = df.reset_index() if df.index.name else df
    df = df.fillna('')    
    if capitalize:
        df = capitalize_column_names_in_df(df)
    data = df.to_dict(orient='records')
    return data

def map_data_to_df(data):
    df = pd.DataFrame(data)
    return df

def map_df_to_csv(df, file_folder, file_name):
    file_path = os.path.join(file_folder, file_name)
    df.to_csv(file_path, index=False)
    print(f"| Saved csv to {file_path}")
    return None

# def map_df_to_json(df, file_folder, file_name):
#     data = map_df_to_data(df)
#     file_path = os.path.join(file_folder, file_name)
#     with open(file_path, 'w') as f:
#         json.dump(data, f, indent=4, ensure_ascii=False)
#     print(f"| Saved json to {file_path}")
#     return None

# def map_data_to_json(data, file_folder, file_name):
#     file_path = os.path.join(file_folder, file_name)
#     with open(file_path, 'w') as f:
#         json.dump(data, f, indent=4, ensure_ascii=False)
#     print(f"| Saved json to {file_path}")
#     return None

def save_df_as_csv(df, file_folder, file_name):
    df.to_csv(os.path.join(file_folder, file_name), index=False)
    print(f"| Saved csv to {os.path.join(file_folder, file_name)}")
    return None

def save_data_as_json(data, file_folder, file_name):
    with open(os.path.join(file_folder, file_name), 'w') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"| Saved json to {os.path.join(file_folder, file_name)}")
    return None

def save_df_as_json(df, file_folder, file_name):
    data = map_df_to_data(df)
    print("| Transformed df to json data")
    save_data_as_json(data, file_folder, file_name)
    print(f"| Saved json to {os.path.join(file_folder, file_name)}")
    return None

map_df_to_csv = save_df_as_csv
map_df_to_json = save_df_as_json
map_data_to_json = save_data_as_json


def rename_columns(df, mapping):
    df = df.rename(columns=mapping)
    df.columns = [col.upper() for col in df.columns]
    return df

def load_json_file(file_name):
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data


def capitalize_column_names_in_df(df):
    cols_ref = df.columns
    df.columns = [col.upper() for col in cols_ref]
    return df

def transfrom_df_to_data_fits_universal_dataframe(df, rnd=2):
    df = round(df, rnd)
    data = map_df_to_data(df, capitalize=True)
    return data
    