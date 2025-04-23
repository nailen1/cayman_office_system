FILE_NAME_PREFIX_TRADE = 'Samsung Securities Co., Ltd.'
FILE_NAME_PREFIX_ORDER = '^Life Asset trade instruction'
FILE_NAME_PREFIX_STATUS = '^Life Asset status'
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
