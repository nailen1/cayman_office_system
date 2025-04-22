from .dataset_operation_loader import open_df_operation_by_end_date
from .number_utils import transform_commaed_string_to_float

def get_net_cashflow_at_month_end(end_date):
    end_date_no_dashed = end_date.replace('-', '')
    df = open_df_operation_by_end_date(end_date_no_dashed)
    net_cashflow = df['SumPeriodToDate'].dropna().unique()[-1]
    float_net_cashflow = transform_commaed_string_to_float(net_cashflow)
    return float_net_cashflow, end_date

def get_sum_of_revenues(end_date):
    df = open_df_operation_by_end_date(end_date)
    revenues = df.copy().loc['Revenues']
    revenues['revenue'] = revenues['PeriodToDate'].apply(lambda x: transform_commaed_string_to_float(x))
    sum_of_revnues = revenues['revenue'].sum(axis=0)
    return sum_of_revnues, end_date

def get_sum_of_expenses(end_date):
    df = open_df_operation_by_end_date(end_date)
    expenses = df.copy().loc['Expenses']
    expenses['expense'] = expenses['PeriodToDate'].apply(lambda x: transform_commaed_string_to_float(x))
    sum_of_expenses = expenses['expense'].sum(axis=0)
    return sum_of_expenses, end_date

def get_sum_of_revenues_and_expenses_at_month_end(end_date):
    net_cashflow, _ = get_net_cashflow_at_month_end(end_date)
    sum_of_revenues, _ = get_sum_of_revenues(end_date)
    sum_of_expenses, _ = get_sum_of_expenses(end_date)
    sum_of_revenues_and_expenses = net_cashflow - sum_of_revenues + sum_of_expenses
    return sum_of_revenues_and_expenses, end_date