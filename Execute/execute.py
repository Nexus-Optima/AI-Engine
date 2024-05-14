import json
from datetime import datetime
from dotenv import load_dotenv

import Constants.constants as cts
import Constants.parameters as prms
from Data_Preprocessing.data_processing import create_features_dataset
from Database.dynamoDB_operations import store_model_details_in_dynamoDB, store_forecast
from Database.s3_operations import read_data_s3, store_models_s3

# Utilities for data processing.
from Utils.process_data import process_data_lagged


# Importing different modeling approaches.
from Models.XG_Boost.adaptive_xgboost import execute_adaptive_xgboost
from Models.ETS import execute_ets
from Models.Arima import execute_arima
from DL_Models.LSTM.LSTM import execute_lstm

load_dotenv()

# Boto3 clients


def forecast_pipeline(commodity_name):
    """
    Run the forecasting pipeline for multiple models.
    """
    read_df = read_data_s3(cts.Commodities.COMMODITIES, commodity_name)
    processed_data = process_data_lagged(read_df, prms.FORECASTING_DAYS)
    features_dataset = create_features_dataset(processed_data.copy())
    features_dataset = features_dataset.last('4Y')

    # Ensure that you're referencing actual function objects here:
    models = {
        'LSTM': {
            'func': execute_lstm,  # This should directly reference the function, not a string or anything else.
            'params': {
                'forecast': prms.FORECASTING_DAYS,
                'hyperparameters': prms.lstm_parameters_4Y_30D
            }
        },
        'ETS': {
            'func': execute_ets,  # This should directly reference the function, not a string or anything else.
            'params': {
                'forecast': prms.FORECASTING_DAYS,
                'hyperparameters': prms.lstm_parameters_4Y_30D
            }
            # Make sure other models are added here similarly.
        },
        'XGBoost': {
            'func': execute_adaptive_xgboost,
            # This should directly reference the function, not a string or anything else.
            'params': {
                'forecast': prms.FORECASTING_DAYS,
                'hyperparameters': prms.xgboost_params_2Y
            }
            # Make sure other models are added here similarly.
        },
        'ARIMA': {
            'func': execute_arima,
            # This should directly reference the function, not a string or anything else.
            'params': {
                'forecast': prms.FORECASTING_DAYS,
                'hyperparameters': prms.xgboost_params_2Y
            }
            # Make sure other models are added here similarly.
        },
        # 'LGBM': {
        #     'func': execute_LGBM,  # This should directly reference the function, not a string or anything else.
        #     'params': {
        #         'forecast': prms.FORECASTING_DAYS,
        #         'hyperparameters': prms.xgboost_params_2Y
        #     }
        # Make sure other models are added here similarly.
        # }
    }

    all_model_details = []
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    s3_path = f'model_runs/{commodity_name}_{current_time}.json'

    # Execute each model
    for model_name, model_info in models.items():
        model_func = model_info['func']
        params = model_info['params']
        actual_values, predictions, forecast_outputs, accuracy = execute_model(
            model_func, read_df, features_dataset, params['forecast'], params['hyperparameters']
        )
        model_details = {
            "model_name": model_name,
            "accuracy": accuracy,
            "hyper_parameters": params['hyperparameters'],
            "input_columns": list(features_dataset.columns),
        }
        all_model_details.append(model_details)
        store_model_details_in_dynamoDB(model_name, accuracy, params['hyperparameters'], list(features_dataset.columns),
                                        s3_path)
        # datasets = {"actual_values": actual_values, "forecast_values": forecast_outputs}
        # store_forecast(cts.Commodities.FORECAST_STORAGE, "cotton", datasets)

    # Store all details in a single S3 file
    store_models_s3(s3_path, json.dumps(all_model_details))


def execute_model(model_func, raw_data, processed_data, forecast, hyperparameters):
    """
    General function to execute any model with the given data, forecast period, and hyperparameters.
    Assumes model_func is a callable that matches this signature.
    """
    actual_values, predictions, forecast_results, accuracy = model_func(raw_data, processed_data, forecast, hyperparameters)
    return actual_values, predictions, forecast_results, accuracy
