import torch
import torch.nn as nn
import pandas as pd
import torch.optim as optim
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
from Utils.process_data import process_data, process_data_lagged
from DL_Models.LSTM.lstm_structure import LSTMModel
from DL_Models.LSTM import lstm_utils


def execute_lstm(raw_data, data, forecast, hyperparameters):
    data = data.reset_index()
    data.drop(columns=['Date'], inplace=True)
    data.dropna(inplace=True)
    scaled_data, data_min, data_max = lstm_utils.min_max_scaler(data.values)
    train_data, test_data = train_test_split(scaled_data, test_size=0.2, shuffle=False)
    look_back = 7
    X_train, y_train = lstm_utils.create_dataset(train_data, look_back)
    X_test, y_test = lstm_utils.create_dataset(test_data, look_back)

    X_train_tensor = torch.FloatTensor(X_train)
    y_train_tensor = torch.FloatTensor(y_train)
    X_test_tensor = torch.FloatTensor(X_test)
    y_test_tensor = torch.FloatTensor(y_test)

    model = LSTMModel(input_size=X_train.shape[2], hyperparameters=hyperparameters)
    criterion = nn.MSELoss(reduction='mean')
    optimizer = optim.Adam(model.parameters(), lr=hyperparameters['lr'], weight_decay=hyperparameters['weight_decay'])

    num_epochs = hyperparameters['num_epochs']
    train_losses, test_losses = [], []

    for epoch in range(num_epochs):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_train_tensor)
        loss = criterion(outputs, y_train_tensor.unsqueeze(1))
        loss.backward()
        optimizer.step()

    model.eval()
    train_predictions = model(X_train_tensor).detach().numpy()
    test_predictions = model(X_test_tensor).detach().numpy()

    train_predictions_orig = lstm_utils.inverse_min_max_scaler(train_predictions, data_min[0], data_max[0])
    y_train_orig = lstm_utils.inverse_min_max_scaler(y_train, data_min[0], data_max[0])
    test_predictions_orig = lstm_utils.inverse_min_max_scaler(test_predictions, data_min[0], data_max[0])
    y_test_orig = lstm_utils.inverse_min_max_scaler(y_test, data_min[0], data_max[0])
    train_mse = np.mean((train_predictions_orig - y_train_orig) ** 2)
    # test_rmse = np.sqrt(np.mean((test_predictions_orig - y_test_orig) ** 2))
    test_rmse = np.sqrt(mean_squared_error(y_test_orig, test_predictions_orig))
    raw_data.reset_index(inplace=True)
    raw_data = process_data(raw_data)
    future_dates = [raw_data.index[-1] + pd.Timedelta(days=i) for i in range(1, forecast + 1)]
    future_data = pd.DataFrame(index=future_dates)
    future_data.index.name = 'Date'
    combined_data = pd.concat([raw_data, future_data])
    combined_data = combined_data.reset_index()
    combined_data = process_data_lagged(combined_data, forecast)
    combined_data = combined_data.last('4Y')
    combined_data = combined_data.reset_index()
    combined_data = combined_data[combined_data['Date'].dt.dayofweek < 5]
    dates = combined_data['Date']
    combined_data.drop(columns=['Date'], inplace=True)

    combined_data = combined_data[data.columns]
    scaled_forecast_data, future_data_min, future_data_max = lstm_utils.min_max_scaler(combined_data.values)
    forecast_values = []
    last_data = scaled_data[-look_back:]
    # print(last_data)
    for i in range(len(future_data)):
        with torch.no_grad():
            model.eval()
            prediction = model(torch.FloatTensor(last_data[-look_back:].reshape(1, look_back, -2)))
            forecast_values.append(prediction.item())
            scaled_forecast_data[i - len(future_data)][0] = prediction.item()
            # remove NaN values to perform proper scaling
            new_row = scaled_forecast_data[i - len(future_data)]
            last_data = np.vstack([last_data, new_row])
    forecast_orig = lstm_utils.inverse_min_max_scaler(np.array(forecast_values).reshape(-1, 1), future_data_min[0],
                                                      future_data_max[0])
    # print(forecast_orig)
    all_actual = np.concatenate([y_train_orig, y_test_orig])
    all_predictions = np.concatenate([train_predictions_orig, test_predictions_orig])
    time_array = np.arange(len(all_actual) + len(forecast_orig))
    plt.figure(figsize=(16, 7))
    plt.plot(dates[:len(all_actual)], all_actual, label='Actual Values', color='blue')
    plt.plot(dates[:len(all_predictions)], all_predictions, label='Predicted Values', color='red', linestyle='--')
    plt.plot(dates[-1*len(forecast_orig):], forecast_orig, label='Forecasted Values', color='green', linestyle='-.')
    plt.title('Actual vs. Predicted vs. Forecasted Values')
    plt.xlabel('Time Steps')
    plt.ylabel('Output Values')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    test_dates = dates[-len(y_test_orig):]  # Adjust this according to how you've split your dates

    # Map test predictions with dates
    test_predictions_df = pd.DataFrame({
        'Date': test_dates,
        'Test Predictions': test_predictions_orig.flatten()  # Flatten in case it's not a 1D array
    })
    y_test_orig_df = pd.DataFrame({
        'Date': test_dates,
        'Actual Values': y_test_orig.flatten()  # Flatten in case it's not a 1D array
    })

    forecast_dates = pd.date_range(start=test_dates.iloc[-1] + pd.Timedelta(days=1), periods=len(forecast_orig))
    forecast_orig_df = pd.DataFrame({
        'Date': forecast_dates,
        'Forecast Values': forecast_orig.flatten()  # Flatten in case it's not a 1D array
    })

    return test_predictions_df, forecast_orig_df, test_rmse
