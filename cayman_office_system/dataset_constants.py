import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join(ROOT_DIR)

DIR_SECTOR = 'dataset-sector'
DIR_ORDER = 'dataset-order'
DIR_BALANCE = 'dataset-balance'
DIR_TRADE = 'dataset-trade'
DIR_STATUS = 'dataset-status'
DIR_HOLDING = 'dataset-holding'
DIR_CURRENCY = 'dataset-currency'
DIR_GENERATE = 'dataset-generate'

FILE_FOLDER_SECTOR = os.path.join(BASE_DIR, DIR_SECTOR)
FILE_FOLDER_ORDER = os.path.join(BASE_DIR, DIR_ORDER)
FILE_FOLDER_BALANCE = os.path.join(BASE_DIR, DIR_BALANCE)
FILE_FOLDER_TRADE = os.path.join(BASE_DIR, DIR_TRADE)
FILE_FOLDER_STATUS = os.path.join(BASE_DIR, DIR_STATUS)
FILE_FOLDER_HOLDING = os.path.join(BASE_DIR, DIR_HOLDING)
FILE_FOLDER_CURRENCY = os.path.join(BASE_DIR, DIR_CURRENCY)
FILE_FOLDER_GENERATE = os.path.join(BASE_DIR, DIR_GENERATE)

file_folder = {
    'sector': FILE_FOLDER_SECTOR,
    'order': FILE_FOLDER_ORDER,
    'balance': FILE_FOLDER_BALANCE,
    'trade': FILE_FOLDER_TRADE,
    'status': FILE_FOLDER_STATUS,
    'holding': FILE_FOLDER_HOLDING,
    'currency': FILE_FOLDER_CURRENCY,
    'generate': FILE_FOLDER_GENERATE
}

FILE_NAME_PREFIX_ORDER = '^Life Asset trade instruction'
FILE_NAME_PREFIX_STATUS = '^Life Asset status'
FILE_NAME_PREFIX_TRADE = '^Life Asset balance'
FILE_NAME_PREFIX_HOLDING = '^dataset-cayman-holding'


COLUMNS_ASOF = ['Name', 'Symbol', '# of Current Shares', 'Purchase Price',
       'Purchase Amount']

COLUMNS_NEW = ['Name', 'New Order']

COLUMNS_AFTER = ['Name', '# of After Order Shares', 'Last Price',
       'After Order Amount(KRW)', 'After Order Amount(USD)',
       'Portfolio Weight']

MAPPING_COLUMNS_ASOF = {
    'Name': 'name',
    'Symbol': 'ticker',
    '# of Current Shares': 'num_asof',
    'Purchase Price': 'price_purchase',
    'Purchase Amount': 'amount_asof'
}

MAPPING_COLUMNS_NEW = {
    'Name': 'name_order',
    'New Order': 'num_order'
}

MAPPING_COLUMNS_AFTER = {
    'Name': 'name',
    '# of After Order Shares': 'num_after',
    'Last Price': 'price_last',
    'After Order Amount(KRW)': 'amount_after_krw',
    'After Order Amount(USD)': 'amount_after_usd',
    'Portfolio Weight': 'weight'
}


MAPPING_COLUMNS_ORDER = {
    'Name': 'name',
    'Symbol': 'ticker',
    'Amount': 'amount'
}

MAPPING_SECTOR = {
    'NAME': 'name',
    'GICS_SECTOR_NAME': 'sector',
    'ticker_bbg_index': 'market_index'
}
