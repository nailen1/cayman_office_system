from .dataset_operation_loader import open_df_operation_by_end_date
from .number_utils import transform_commaed_string_to_float

def get_net_cashflow_at_month_end(end_date):
    end_date_no_dashed = end_date.replace('-', '')
    df = open_df_operation_by_end_date(end_date_no_dashed)
    net_cashflow = df['SumPeriodToDate'].dropna().unique()[-1]
    float_net_cashflow = transform_commaed_string_to_float(net_cashflow)
    return float_net_cashflow, end_date
