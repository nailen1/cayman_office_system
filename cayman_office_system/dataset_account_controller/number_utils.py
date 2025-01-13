def transform_value_to_float(value):
    try:
        number = float(value)
        return number
    except (ValueError, TypeError):
        return value

def transform_commaed_string_to_float(commaed_string):
    try:
        value = float(commaed_string.replace(',', ''))
    except:
        value = None
    return value

def transform_commaed_string_to_int(commaed_string):
    try:
        value = int(commaed_string.replace(',', ''))
    except:
        value = None
    return value

def transform_number_like_value_to_float(value):
    try:
        return transform_commaed_string_to_float(value)
    except:
        return transform_value_to_float(value)
