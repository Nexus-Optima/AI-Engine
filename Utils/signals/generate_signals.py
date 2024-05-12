import pandas as pd


def generate_signals(data):
    short_window = 5
    long_window = 20

    signals = pd.DataFrame(index=data.index)
    signals['price'] = data
    signals['short_mavg'] = data.rolling(window=short_window).mean()
    signals['long_mavg'] = data.rolling(window=long_window).mean()
    signals['signal'] = 0.0
    signals.loc[signals['short_mavg'] > signals['long_mavg'], 'signal'] = 1.0
    signals['positions'] = signals['signal'].diff()
    return signals['positions'].tolist()


def modified_generate_signals(data):
    if isinstance(data, pd.Series):
        data = data.to_frame(name='Output')

    data['diff'] = data['Output'].diff()
    window_size_diff = 7
    data['smoothed_diff'] = data['diff'].rolling(window=window_size_diff).mean()
    buy_threshold = data['smoothed_diff'].quantile(0.95)
    sell_threshold = data['smoothed_diff'].quantile(0.05)
    data['signal'] = 0
    data.loc[data['smoothed_diff'] > buy_threshold, 'signal'] = 1
    data.loc[data['smoothed_diff'] < sell_threshold, 'signal'] = -1

    return data['signal'].tolist()
