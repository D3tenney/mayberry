def date_from_string(input):
    try:
        len(input) == 14
        output = f'{input[:4]}-{input[4:6]}-{input[6:8]}'
        return output
    except Exception as e:
        return f'date from string error: {e}'
