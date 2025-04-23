import os

# ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.join('data-cos')

DIR_SECTOR = 'dataset-sector'
DIR_MARKET = 'dataset-market'
DIR_ORDER = 'dataset-order'
DIR_BALANCE = 'dataset-balance'
DIR_TRADE = 'dataset-trade'
DIR_STATUS = 'dataset-status'
DIR_HOLDING = 'dataset-holding'
DIR_CURRENCY = 'dataset-currency'
DIR_GENERATE = 'dataset-generate'
DIR_BBG = 'dataset-bbg'
DIR_ACCOUNT = 'dataset-account'
DIR_ACCOUNT_MASTER = 'dataset-account/lkef-master'
DIR_ACCOUNT_FEEDER1 = 'dataset-account/lkef-feeder1'
DIR_ACCOUNT_FEEDER2 = 'dataset-account/lkef-feeder2'
DIR_ACCOUNT_RAW = 'dataset-account/dataset-account-raw'
DIR_OPERATION = 'dataset-operation'


FILE_FOLDER_SECTOR = os.path.join(BASE_DIR, DIR_SECTOR)
FILE_FOLDER_MARKET = os.path.join(BASE_DIR, DIR_MARKET)
FILE_FOLDER_ORDER = os.path.join(BASE_DIR, DIR_ORDER)
FILE_FOLDER_BALANCE = os.path.join(BASE_DIR, DIR_BALANCE)
FILE_FOLDER_TRADE = os.path.join(BASE_DIR, DIR_TRADE)
FILE_FOLDER_STATUS = os.path.join(BASE_DIR, DIR_STATUS)
FILE_FOLDER_HOLDING = os.path.join(BASE_DIR, DIR_HOLDING)
FILE_FOLDER_CURRENCY = os.path.join(BASE_DIR, DIR_CURRENCY)
FILE_FOLDER_GENERATE = os.path.join(BASE_DIR, DIR_GENERATE)
FILE_FOLDER_BBG = os.path.join(BASE_DIR, DIR_BBG)
FILE_FOLDER_ACCOUNT = os.path.join(BASE_DIR, DIR_ACCOUNT)
FILE_FOLDER_ACCOUNT_MASTER = os.path.join(BASE_DIR, DIR_ACCOUNT_MASTER)
FILE_FOLDER_ACCOUNT_FEEDER1 = os.path.join(BASE_DIR, DIR_ACCOUNT_FEEDER1)
FILE_FOLDER_ACCOUNT_FEEDER2 = os.path.join(BASE_DIR, DIR_ACCOUNT_FEEDER2)
FILE_FOLDER_ACCOUNT_RAW = os.path.join(BASE_DIR, DIR_ACCOUNT_RAW)
FILE_FOLDER_OPERATION = os.path.join(BASE_DIR, DIR_OPERATION)

file_folder = {
    'sector': FILE_FOLDER_SECTOR,
    'market': FILE_FOLDER_MARKET,
    'order': FILE_FOLDER_ORDER,
    'balance': FILE_FOLDER_BALANCE,
    'trade': FILE_FOLDER_TRADE,
    'status': FILE_FOLDER_STATUS,
    'holding': FILE_FOLDER_HOLDING,
    'currency': FILE_FOLDER_CURRENCY,
    'generate': FILE_FOLDER_GENERATE,
    'bbg': FILE_FOLDER_BBG,
    'account': FILE_FOLDER_ACCOUNT,
    'master': FILE_FOLDER_ACCOUNT_MASTER,
    'feeder1': FILE_FOLDER_ACCOUNT_FEEDER1,
    'feeder2': FILE_FOLDER_ACCOUNT_FEEDER2,
    'account-raw': FILE_FOLDER_ACCOUNT_RAW,
    'operation': FILE_FOLDER_OPERATION
}
