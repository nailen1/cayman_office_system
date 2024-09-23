import pandas as pd
from datetime import datetime
import calendar


def get_ticker_bbg_of_ticker(ticker):
    return f"{ticker} KS Equity".replace(' KS KS', ' KS')


def convert_to_unit(number, currency='KRW', level=None):
    if pd.isna(number) or number == "NaN":
        return number

    number = abs(float(str(number).replace(',', '')))
    
    if currency.upper() == 'KRW':
        units = [
            (1e12, '조'),
            (1e8, '억'),
            (1e4, '만'),
            (1, '')
        ]
    elif currency.upper() == 'USD': 
        units = [
            (1e12, 'T'),
            (1e9, 'B'),
            (1e6, 'M'),
            (1e3, 'K'),
            (1, '')
        ]

    if level is None:
        level = len(units)

    result = []
    for i, (unit_value, unit_name) in enumerate(units[:level]):
        unit_count = int(number // unit_value)
        if unit_count > 0:
            result.append(f"{unit_count}{unit_name}")
        number %= unit_value

    if not result:
        return '0'

    return ' '.join(result)


def format_number(number):
    return f"{number:,}"


def get_last_day_of_month(date_str):
    # 입력 형식 감지
    if '-' in date_str:
        date_format = "%Y-%m-%d"
        output_format = "%Y-%m-%d"
    else:
        date_format = "%Y%m%d"
        output_format = "%Y%m%d"
    
    # 문자열을 datetime 객체로 변환
    date_obj = datetime.strptime(date_str, date_format)
    
    # 해당 월의 마지막 날 계산
    last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
    
    # 마지막 날의 날짜 객체 생성
    last_date_obj = datetime(date_obj.year, date_obj.month, last_day)
    
    # 지정된 형식으로 변환하여 반환
    return last_date_obj.strftime(output_format)

# 예제 사용
date_str1 = '20231015'
print(get_last_day_of_month(date_str1))  # 출력: 20231031

date_str2 = '2023-10-15'
print(get_last_day_of_month(date_str2))  # 출력: 2023-10-31