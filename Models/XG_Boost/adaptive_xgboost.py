import xgboost as xgb
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pandas as pd
import numpy as np
from Utils.process_data import process_data_lagged, process_data_lagged_rolling_stats
from Utils.visualise_graph import plot_graph


def execute_evaluation(subset_data, hyperparams):
    """
    Evaluate the XGBoost model on the provided data. Uses a moving window approach.

    Parameters:
    - subset_data (pd.DataFrame): The subset of data to evaluate on.

    Returns:
    - actual_values (list): Actual values of the time series for the evaluation period.
    - predictions (list): Predicted values of the time series for the evaluation period.
    """
    predictions = []
    actual_values = []
    window_size = int(0.5 * len(subset_data))

    model = xgb.XGBRegressor(
        n_estimators=hyperparams['n_estimators'],
        max_depth=hyperparams['max_depth'],
        learning_rate=hyperparams['learning_rate'],
        subsample=hyperparams['subsample'],
        colsample_bytree=hyperparams['colsample_bytree'],
        n_jobs=-1,
        objective='reg:squarederror',
        random_state=42,
        early_stopping_rounds=50
    )

    # Moving window approach to train and validate the model
    for window_start in range(0, len(subset_data) - window_size):
        train_data = subset_data.iloc[window_start:window_start + window_size]
        val_data = subset_data.iloc[window_start + window_size:window_start + window_size + 1]

        X_train = train_data.drop(columns='Output')
        X_train = X_train[[col for col in X_train.columns if '_lag' in col]]

        X_val = val_data.drop(columns='Output')
        X_val = X_val[[col for col in X_val.columns if '_lag' in col]]

        y_train = train_data['Output']
        y_val = val_data['Output']

        model.fit(X_train, y_train, eval_set=[(X_train, y_train), (X_val, y_val)])
        trained_feature_order = X_train.columns.tolist()

        predictions.extend(model.predict(X_val))
        actual_values.extend(y_val.values)

    plot_graph(subset_data.index[window_size:window_size + len(predictions)], actual_values, predictions)

    XGB_rmse = np.sqrt(mean_squared_error(actual_values, predictions))
    print(mean_absolute_error(actual_values, predictions))

    return actual_values, predictions, trained_feature_order, XGB_rmse


def execute_adaptive_xgboost(raw_data, subset_data, forecast_days, hyperparams):
    """
    Execute adaptive XGBoost on the given dataset to predict future values.

    Parameters:
    - data (pd.DataFrame): The input dataset with time as index and features as columns.
    - forecast_days (int): Number of days to forecast into the future.

    Returns:
    - predictions (list): List of predicted values.
    """

    # Determine the window size as half the length of the subset data
    window_size = int(0.5 * len(subset_data))

    # Retrieve actual values and predictions for the subset data
    actual_values, predictions, feature_order, XGB_rmse = execute_evaluation(subset_data, hyperparams)

    # Split the data into training sets
    train_data = subset_data[-window_size:]
    X_train, y_train = train_data.drop(columns='Output'), train_data['Output']
    X_train = X_train[[col for col in X_train.columns if '_lag' in col]]

    # Initialize the XGBoost model
    model_future = xgb.XGBRegressor(
        n_estimators=hyperparams['n_estimators'],
        max_depth=hyperparams['max_depth'],
        learning_rate=hyperparams['learning_rate'],
        subsample=hyperparams['subsample'],
        colsample_bytree=hyperparams['colsample_bytree'],
        n_jobs=-1,
        objective='reg:squarederror',
        random_state=42,
        early_stopping_rounds=50
    )
    model_future.fit(X_train, y_train, eval_set=[(X_train, y_train)])

    # Generate future dates for prediction
    future_dates = [subset_data.index[-1] + pd.Timedelta(days=i) for i in range(1, forecast_days + 1)]
    future_data = pd.DataFrame(columns=X_train.columns, index=future_dates)

    # Populate initial values for the future data based on the last known values
    for col in future_data.columns:
        if col in subset_data.columns:
            future_data.at[future_data.index[0], col] = subset_data.at[subset_data.index[-1], col]

    # Prepare the concatenated data for iterative prediction
    complete_data = raw_data.copy()

    # Iterate over each forecast day and predict values
    for i in range(forecast_days):
        X_next = future_data.iloc[[i]].drop(columns='Output', errors='ignore')
        X_next = X_next[feature_order]

        # Convert object columns to numeric
        for col in X_next.columns:
            if X_next[col].dtype == 'object':
                X_next[col] = pd.to_numeric(X_next[col], errors='coerce')

        # Predict the next value
        prediction = model_future.predict(X_next)
        future_data.loc[future_data.index[i], 'Output'] = prediction[0]

        # Add the predicted row to concatenated data
        next_row = X_next.copy()
        next_row['Output'] = prediction[0]

        next_row_complete = complete_data.iloc[-1].copy()
        next_row_complete.update(next_row.iloc[0])
        next_row_complete['Output'] = prediction[0]

        complete_data = pd.concat([complete_data, next_row_complete.to_frame().T])

        # If not the last iteration, update future_data with lagged values
        if i < forecast_days - 1:
            complete_data.reset_index(inplace=True, drop=False)
            complete_data.rename(columns={'index': 'Date'}, inplace=True)
            complete_data = process_data_lagged(complete_data, forecast_days)

            prediction_data = complete_data[future_data.columns]

            next_day_row = prediction_data.iloc[-1].copy()
            next_day_row = next_day_row[future_data.columns]
            future_data.iloc[i + 1] = next_day_row

    past_dates = subset_data.index[window_size:window_size + len(predictions)]
    future_dates_list = future_data.index.tolist()

    plot_graph(past_dates, actual_values, predictions, future_data['Output'].tolist(), future_dates_list)


    return actual_values, predictions, future_data['Output'], XGB_rmse
