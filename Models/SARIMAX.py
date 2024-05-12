import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error


def execute_sarimax(data, forecast_days):
    train_size = int(0.8 * len(data))
    train, test = data.iloc[:train_size], data.iloc[train_size:]

    X_train, X_test = train.drop("Output", axis=1), test.drop("Output", axis=1)
    y_train, y_test = train["Output"], test["Output"]

    sarimax_model = SARIMAX(y_train, exog=X_train, order=(1, 1, 1), seasonal_order=(1, 1, 1, 365))
    sarimax_result = sarimax_model.fit(disp=False)

    sarimax_predictions = sarimax_result.predict(start=y_test.index[0], end=y_test.index[-1], exog=X_test)

    sarimax_rmse = np.sqrt(mean_squared_error(y_test, sarimax_predictions))
    print(f"RMSE for SARIMAX model: {sarimax_rmse:.4f}")

    forecast_data = data.iloc[-len(y_test):].copy()
    exog_forecast = forecast_data[X_train.columns]
    sarimax_forecast = sarimax_result.get_forecast(steps=forecast_days, exog=exog_forecast).predicted_mean

    print(sarimax_forecast)

    plt.figure(figsize=(16, 8))
    plt.plot(data['Output'], label="Actual Data", color="blue")
    plt.plot(y_test.index, sarimax_predictions, label="SARIMAX Predictions", color="red", linestyle="--")
    forecast_index = pd.date_range(data.index[-1], periods=forecast_days + 1, inclusive='right')
    plt.plot(forecast_index, sarimax_forecast, 'g--', label="45-day SARIMAX Forecast", alpha=0.7)
    plt.title("Cotlook A Index with SARIMAX Predictions and 45 Days Forecast")
    plt.xlabel("Date")
    plt.ylabel("Cotlook A Index")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    return sarimax_forecast
