import pandas as pd


def process_data(data):
    data['Date'] = pd.to_datetime(data['Date'], format='%d/%m/%y')
    data.set_index('Date', inplace=True)
    data.sort_index(inplace=True)
    return data


def process_data_lagged(data, forecast_days):
    data = process_data(data)
    lags = [1, 5, 7, 15, 30]

    for col in data.columns:
        if "_lag" in col:
            data.drop(col, axis=1, inplace=True)

    for col in data.columns:
        for lag in lags:
            if lag >= forecast_days:
                lag_col_name = f"{col}_lag{lag}"
                data[lag_col_name] = data[col].shift(lag)

    data.dropna(inplace=True)
    return data


def process_data_lagged_rolling_stats(data, forecast_days):
    data = process_data_lagged(data, forecast_days)
    window_sizes = [3, 14, 30]

    for window in window_sizes:
        data[f'Output_lag_{window}d_mean'] = data['Output'].rolling(window=window).mean()
        data[f'Output_lag_{window}d_std'] = data['Output'].rolling(window=window).std()

    data.dropna(inplace=True)
    return data


'''
Add Expanding Window (IF NEEDED)
'''
