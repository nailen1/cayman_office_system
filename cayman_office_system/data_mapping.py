import pandas as pd

def map_df_to_data(df):
    df = df.reset_index() if df.index.name else df
    data = df.to_dict(orient='records')
    return data

def map_data_to_df(data):
    df = pd.DataFrame(data)
    return df