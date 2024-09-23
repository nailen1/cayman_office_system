import mysql.connector
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
host = os.getenv("MYSQL_HOST")
database = os.getenv("MYSQL_DATABASE_NAME")

connection = mysql.connector.connect(user=user, password=password, host=host, database=database)
cursor = connection.cursor()

MAPPING_LATEST_TABLE_COLUMNS = {
    'scode': 'code',
    'sname': 'name_kr',
    'last_close': 'price_last',
    'marketcap': 'cap(/1e8)'
}

MAPPING_TIMESERIES_TABLE_COLUMNS = {
    'dt': 'date', 
    'pr1': 'price_last',
} 

COLUMNS_FOR_PRICE = [k for k, v in MAPPING_LATEST_TABLE_COLUMNS.items()]
COLUMNS_FOR_TIMESERIES = [k for k, v in MAPPING_TIMESERIES_TABLE_COLUMNS.items()]

def get_data_of_ks_stock(cols):
    connection = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = connection.cursor()
    query = f"SELECT {', '.join(cols)} FROM ks_stocks"
    cursor.execute(query)
    data = cursor.fetchall()
    return data

def preprocess_df_from_price_data(data):
    df = pd.DataFrame(data, columns=COLUMNS_FOR_PRICE)
    df = df.rename(columns=MAPPING_LATEST_TABLE_COLUMNS)
    df['ticker_bbg'] = df['code'].apply(lambda x: f"{x} KS Equity")
    df = df.drop(columns=['code']).set_index('ticker_bbg')
    return df

def get_df_prices_of_ks_stock():
    data = get_data_of_ks_stock(COLUMNS_FOR_PRICE)
    df = preprocess_df_from_price_data(data)
    return df

def get_data_of_timeseries_by_ticker(ticker, cols):
    connection = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = connection.cursor()
    ticker = ticker[:6]
    query = f"SELECT {', '.join(cols)} FROM p_ks_{ticker}"
    cursor.execute(query)
    data = cursor.fetchall()
    return data

def preprocess_df_from_timeseries_data(data):
    df = pd.DataFrame(data, columns=COLUMNS_FOR_TIMESERIES)
    df = df.rename(columns=MAPPING_TIMESERIES_TABLE_COLUMNS)
    df = df.set_index('date')
    df.index = df.index.map(lambda x: str(x))
    return df

def get_df_timeseries_by_ticker(ticker):
    data = get_data_of_timeseries_by_ticker(ticker, COLUMNS_FOR_TIMESERIES)
    df = preprocess_df_from_timeseries_data(data)
    return df

def get_price_of_date_in_df(df, date):
    date = str(date)
    price = df.loc[date, 'price_last']
    return price

# Function Aliasing
def get_price_by_ticker(ticker):
    df = get_df_timeseries_by_ticker(ticker)
    return df

def get_price_of_date_by_ticker(ticker, date):
    connection = mysql.connector.connect(user=user, password=password, host=host, database=database)
    cursor = connection.cursor()
    ticker = ticker[:6]
    query = f"SELECT dt, pr1 FROM p_ks_{ticker} WHERE dt = '{date}'"
    cursor.execute(query)
    data = cursor.fetchall()
    price_of_date = data[-1][-1]
    return price_of_date