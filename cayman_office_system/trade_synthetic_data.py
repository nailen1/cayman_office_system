SELL_DN = {
    'date': '2024-10-07',
    'ticker': '007340 KS',
    'type': 'Sell',
    'num_share': 29231,
}

SELL_DONGATIRE = {
    'date': '2024-10-07',
    'ticker': '282690 KS',
    'type': 'Sell',
    'num_share': 2500,
    'trades_history': None
}

BUY_DN = {
    'date': '2024-10-08',
    'ticker': '007340 KS',
    'type': 'Buy',
    'num_share': 148100,
    'trades_history': None
}

DNDONGA_MERGER = [SELL_DN, SELL_DONGATIRE, BUY_DN]


# EVENT LOG

# ['date', 'name', 'ticker', 'type', 'num_shares', 'average_price',
#        'consideration', 'commission', 'net_amount', 'delta_shares',
#        'cashflow']

# {'date': '2024-10-07',
#  'name': 'DN AUTOMOTIVE CORP',
#  'ticker': '007340 KS',
#  'type': 'Sell_synthetic',
#  'num_shares': 29231,
#  'average_price': 102100,
#  'consideration': 2984485100,
#  'comission': 0.0,
#  'net_amount': 2984485100.0}

# {'date': '2024-10-07',
#  'name': 'DONG AH TIRE & RUBBER CO LTD',
#  'ticker': '282690 KS',
#  'type': 'Sell_synthetic',
#  'num_shares': 2500,
#  'average_price': 13500,
#  'consideration': 33750000,
#  'comission': 0.0,
#  'net_amount': 33750000.0}

# {'date': '2024-10-08',
#  'name': 'DN AUTOMOTIVE CORP',
#  'ticker': '007340 KS',
#  'type': 'Buy_synthetic',
#  'num_shares': 148100,
#  'average_price': 19200,
#  'consideration': 2843520000,
#  'comission': 0.0,
#  'net_amount': 2843520000.0}

# hotfix trades data composer
# DN, Donga 
