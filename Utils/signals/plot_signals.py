import matplotlib.pyplot as plt
import pandas as pd
from Utils.signals.generate_signals import modified_generate_signals


def plot_predicted_data(predictions, subset_data, predicted_output, future_data, window_size):
    predicted_signals = modified_generate_signals(pd.Series(predictions))

    dates = pd.date_range(start=subset_data.index[window_size], periods=len(predictions), freq='D')

    forecasted_series = pd.Series(predicted_output, index=future_data.index)
    forecasted_signals = modified_generate_signals(forecasted_series)

    predicted_series = pd.Series(predictions, index=dates)
    predicted_signals = pd.Series(predicted_signals)
    predicted_signals.index = predicted_series.index

    forecasted_signals = pd.Series(forecasted_signals)
    forecasted_signals.index = forecasted_series.index

    plt.figure(figsize=(14, 7))
    plt.plot(predicted_series, label="Predicted Values", color='red', linestyle='--')
    plt.plot(forecasted_series, label="Forecast", color='green', linestyle='-.')

    buy_signals = predicted_signals[predicted_signals == 1.0].index
    sell_signals = predicted_signals[predicted_signals == -1.0].index

    plt.scatter(buy_signals, predicted_series[buy_signals], label='BUY Signal', marker='^', color='g', alpha=1, s=100)
    plt.scatter(sell_signals, predicted_series[sell_signals], label='SELL Signal', marker='v', color='r', alpha=1,
                s=100)
    plt.title("Predicted and Forecasted Values with Buy/Sell Signals")
    plt.xlabel("Date")
    plt.ylabel("Output")
    plt.legend()
    plt.tight_layout()
    plt.grid(True)
    plt.show()
