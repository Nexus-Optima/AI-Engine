import matplotlib.pyplot as plt


def plot_graph(dates, actual_values, predictions, forecast_values=None, future_dates=None):
    """
    Plot the actual, predicted, and (optional) forecasted values.

    Parameters:
    - dates (list): Dates corresponding to the actual and predicted values.
    - actual_values (list): Actual values of the time series.
    - predictions (list): Predicted values of the time series.
    - forecast_values (list, optional): Forecasted values for the future. Default is None.
    - future_dates (list, optional): Dates corresponding to the forecasted values. Default is None.
    """
    plt.figure(figsize=(14, 7))
    plt.plot(dates, actual_values, label="Actual Values", color='blue')
    plt.plot(dates, predictions, label="Predicted Values", color='red', linestyle='--')
    if forecast_values:
        plt.plot(future_dates, forecast_values, label="Forecast for Next Days", color='green', linestyle='-.')
    plt.title("Actual vs. Predicted Values")
    plt.xlabel("Date")
    plt.ylabel("Output")
    plt.legend()
    plt.tight_layout()
    plt.grid(True)
    plt.show()
