from .birdeye_connector import get_price_of_date_by_ticker
from .market_information import get_ks_equity_info
from .trades import *


class SyntheticTrade:
    def __init__(self, date=None, ticker=None, type=None, num_share=None, trades_history=None, trade_data=None, **kwargs):
        if isinstance(trade_data, dict):
            self.date = trade_data.get('date')
            self.ticker = trade_data.get('ticker')
            self.type = trade_data.get('type')
            self.num_share = int(trade_data.get('num_share', 0))
            self.trades_history = trade_data.get('trades_history')
        else:
            self.date = date
            self.ticker = ticker
            self.type = type
            self.num_share = int(num_share) if num_share is not None else None
            self.trades_history = trades_history

        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)

        self.df = self.get_df_synthetic_trade()
        self.cash_flow = {'date': self.date, 'cash_flow': self.cash_folw}


    def get_df_synthetic_trade(self):
        print(f'| append synthetic trade of {self.type} (date) = ({self.date})')
        if 'Sell' in self.type:
            dct = compose_data_synthetic_sell_of_date(self.ticker, self.date, self.num_share, self.trades_history)
        elif 'Buy' in self.type:
            dct = compose_data_systhetic_buy_of_date(self.ticker, self.date, self.num_share)
        else:
            raise ValueError(f"Invalid trade type: {self.type}. Must be 'Buy' or 'Sell'.")

        df = pd.DataFrame([dct])
        self.tickers = list(df['ticker'])
        self.names = list(df['name'])
        self.net_amount = df['net_amount'].sum()
        self.cash_folw = df['cash_flow'].sum()
        print(f'|- cash flow of synthetic trade: {self.cash_folw}')
        return df


def compose_data_synthetic_sell_of_date(ticker, date, num_shares=None, trades_history=None):
    name = get_ks_equity_info([ticker]).iloc[-1]['name']
    type = 'Sell_synthetic'
    if not num_shares:
        trades_history = trades_history[trades_history['ticker'] == ticker]
        trades_history = trades_history[trades_history['date'] <= date]
        num_shares = trades_history['num_shares'].sum() # 전량 매도로 간주
    price_of_date = get_price_of_date_by_ticker(date=date, ticker=ticker)
    consideration = num_shares * price_of_date
    # print(f'|- consideration: {consideration}')
    commission = 0.0
    net_amount = consideration + commission
    sign = -1 if 'Sell' in type else 1
    delta_shares = sign * num_shares
    cash_flow = -sign * net_amount

    dct = {
        'date': date,
        'name': name,
        'ticker': ticker,
        'type': type,
        'num_shares': num_shares,
        'average_price': price_of_date,
        'consideration': consideration,
        'commission': commission,
        'net_amount': net_amount,
        'delta_shares': delta_shares,
        'cash_flow': cash_flow,
    }
    return dct

def compose_data_systhetic_buy_of_date(ticker, date, num_shares):
    name = get_ks_equity_info([ticker]).iloc[-1]['name']
    type = 'Buy_synthetic'
    price_of_date = get_price_of_date_by_ticker(date=date, ticker=ticker)
    consideration = num_shares * price_of_date
    commission = 0.0
    net_amount = consideration + commission
    sign = -1 if 'Sell' in type else 1
    delta_shares = sign * num_shares
    cash_flow = -sign * net_amount

    dct = {
        'date': date,
        'name': name,
        'ticker': ticker,
        'type': type,
        'num_shares': num_shares,
        'average_price': price_of_date,
        'consideration': consideration,
        'commission': commission,
        'net_amount': net_amount,
        'delta_shares': delta_shares,
        'cash_flow': cash_flow,
    }
    return dct

