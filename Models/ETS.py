import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.metrics import mean_squared_error


def execute_ets(raw_data, data, forecast_days, hyperparameters):
    data.sort_index(inplace=True)
    Q1 = data['Output'].quantile(0.25)
    Q3 = data['Output'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    data['Output'] = data['Output'].where((data['Output'] >= lower_bound) &
                                          (data['Output'] <= upper_bound))
    data['Output'].interpolate(method='linear', inplace=True)

    train_size = int(0.8 * len(data))
    train, test = data['Output'][:train_size], data['Output'][train_size:]

    model = ExponentialSmoothing(train, trend='add', seasonal='add', seasonal_periods=365, damped_trend=True)
    model_fit = model.fit()

    predictions = model_fit.forecast(steps=len(test))
    rmse = np.sqrt(mean_squared_error(test, predictions))

    forecast_next_100_days = model_fit.forecast(steps=forecast_days)

    plt.figure(figsize=(14, 7))
    plt.plot(train.index, train, label="Training Data", color="blue")
    plt.plot(test.index, test, label="Test Data", color="green")
    plt.plot(test.index, predictions, label="Adjusted ETS Predictions", color="red", linestyle="--")
    forecast_index_100 = pd.date_range(test.index[-1], periods=forecast_days + 1, inclusive='right')
    plt.plot(forecast_index_100, forecast_next_100_days, 'r--', label="100-day Forecast (Adjusted)", alpha=0.7)
    plt.title("Output with Adjusted ETS Model Predictions and 100 Days Forecast")
    plt.xlabel("Date")
    plt.ylabel("Output")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    return predictions, forecast_next_100_days,rmse
