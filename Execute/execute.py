import json
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

import Constants.constants as cts
import Constants.parameters as prms
from Data_Preprocessing.data_processing import create_features_dataset
from Database.dynamoDB_operations import store_model_details_in_dynamoDB, store_forecast
from Database.s3_operations import read_data_s3, store_models_s3

# Utilities for data processing.
from Utils.process_data import process_data_lagged, process_data_weekly

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

    forecast_outputs_dict = {
        "7D": pd.DataFrame(),
        "15D": pd.DataFrame(),
        "30D": pd.DataFrame()
    }

    for i in range(0, len(prms.FORECASTING_DAYS)):

        read_df = read_data_s3(cts.Commodities.COMMODITIES, commodity_name)
        processed_data = process_data_lagged(read_df, prms.FORECASTING_DAYS[i])
        features_dataset = create_features_dataset(processed_data.copy())
        features_dataset = features_dataset.last('4Y')

        # Ensure that you're referencing actual function objects here:
        models = {
            'LSTM': {
                'func': execute_lstm,  # This should directly reference the function, not a string or anything else.
                'model_data': {
                    'initial_data': read_df,
                    'final_data': features_dataset
                },
                'params': {
                    'forecast': prms.FORECASTING_DAYS[i],
                    'hyperparameters': prms.lstm_parameters_days[i]
                }
            },
            # 'LSTM_Weekly': {
            #     'func': execute_lstm,  # This should directly reference the function, not a string or anything else.
            #     'model_data': {
            #         'initial_data': processed_data_weekly,
            #         'final_data': features_dataset_weekly
            #     },
            #     'params': {
            #         'forecast': prms.FORECASTING_WEEKS,
            #         'hyperparameters': prms.lstm_parameters_4Y_30D
            #     }
            # },
            # 'ETS': {
            #     'func': execute_ets,  # This should directly reference the function, not a string or anything else.
            #     'params': {
            #         'forecast': prms.FORECASTING_DAYS,
            #         'hyperparameters': prms.lstm_parameters_4Y_30D
            #     }
            #     # Make sure other models are added here similarly.
            # },
            # 'XGBoost': {
            #     'func': execute_adaptive_xgboost,
            #     # This should directly reference the function, not a string or anything else.
            #     'model_data': {
            #         'initial_data': processed_data,
            #         'final_data': features_dataset
            #     },
            #     'params': {
            #         'forecast': prms.FORECASTING_DAYS,
            #         'hyperparameters': prms.xgboost_params_2Y
            #     }
            #     # Make sure other models are added here similarly.
            # },
            # 'XGBoost_Weekly': {
            #     'func': execute_adaptive_xgboost,
            #     # This should directly reference the function, not a string or anything else.
            #     'model_data': {
            #         'initial_data': processed_data_weekly,
            #         'final_data': features_dataset_weekly
            #     },
            #     'params': {
            #         'forecast': prms.FORECASTING_WEEKS,
            #         'hyperparameters': prms.xgboost_params_2Y
            #     }
            #     # Make sure other models are added here similarly.
            # }
            # 'ARIMA': {
            #     'func': execute_arima,
            #     # This should directly reference the function, not a string or anything else.
            #     'model_data': {
            #         'initial_data': processed_data,
            #         'final_data': features_dataset
            #     },
            #     'params': {
            #         'forecast': prms.FORECASTING_DAYS,
            #         'hyperparameters': prms.xgboost_params_2Y
            #     }
            #     # Make sure other models are added here similarly.
            # },
            # 'ARIMA_Weekly': {
            #     'func': execute_arima,
            #     # This should directly reference the function, not a string or anything else.
            #     'model_data': {
            #         'initial_data': processed_data_weekly,
            #         'final_data': features_dataset_weekly
            #     },
            #     'params': {
            #         'forecast': prms.FORECASTING_WEEKS,
            #         'hyperparameters': prms.xgboost_params_2Y
            #     }
            # Make sure other models are added here similarly.
            # }
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

        # models = {
        #     'LSTM': {
        #         'func': execute_lstm,  # This should directly reference the function, not a string or anything else.
        #         'model_data': {
        #             'initial_data': read_df,
        #             'final_data': features_dataset
        #         },
        #         'params': {
        #             'forecast': prms.FORECASTING_DAYS,
        #             'hyperparameters': prms.lstm_parameters_4Y_30D
        #         }
        #     },


        for model_name, model_info in models.items():
            model_func = model_info['func']
            model_data = model_info['model_data']
            params = model_info['params']

            actual_values, predictions, forecast_outputs, accuracy = execute_model(
                model_func, model_data['initial_data'], model_data['final_data'], params['forecast'],
                params['hyperparameters']
            )

            print(type(forecast_outputs))

            if params['forecast'] == 7:
                forecast_outputs_dict["7D"] = forecast_outputs
            elif params['forecast'] == 15:
                forecast_outputs_dict["15D"] = forecast_outputs
                # Merge the first 7 days from 7D with the remaining 8 days from 15D
                forecast_outputs_dict["15D"] = pd.concat([forecast_outputs_dict["7D"], forecast_outputs.iloc[7:]])
            elif params['forecast'] == 30:
                forecast_outputs_dict["30D"] = forecast_outputs
                # Append the first 15 days from 15D with the remaining 15 days from 30D
                forecast_outputs_dict["30D"] = pd.concat([forecast_outputs_dict["15D"], forecast_outputs.iloc[15:]])


            model_details = {
                "model_name": f"{model_name}_4Y_{params['forecast']}D",
                "accuracy": accuracy,
                "hyper_parameters": params['hyperparameters'],
                "input_columns": list(model_data['final_data'].columns),
            }
            all_model_details.append(model_details)
            store_model_details_in_dynamoDB(model_details['model_name'], accuracy, params['hyperparameters'],
                                            list(model_data['final_data'].columns), s3_path)
            forecast_path = determine_forecast_path(commodity_name, model_name, params['forecast'])
            datasets = {"actual_values": actual_values,
                        "forecast_values": (
                            forecast_outputs_dict["7D"] if params['forecast'] == 7 else
                            forecast_outputs_dict["15D"] if params['forecast'] == 15 else
                            forecast_outputs_dict["30D"] if params['forecast'] == 30 else
                            forecast_outputs
                        ),
                        "prediction_values":predictions}
            store_forecast(forecast_path, datasets)

            # Store all details in a single S3 file
        store_models_s3(s3_path, json.dumps(all_model_details))


def determine_forecast_path(commodity_name, model_name, duration):
    """
    Determine the storage path for forecasts based on the model name.
    """
    if "Weekly" in model_name:
        return f"{cts.Commodities.FORECAST_STORAGE}/{commodity_name}/macro/{duration}W"
    else:
        return f"{cts.Commodities.FORECAST_STORAGE}/{commodity_name}/micro/{duration}D"

def execute_model(model_func, raw_data, processed_data, forecast, hyperparameters):
    """
    General function to execute any model with the given data, forecast period, and hyperparameters.
    Assumes model_func is a callable that matches this signature.
    """
    actual_values, predictions, forecast_results, accuracy = model_func(raw_data, processed_data, forecast,
                                                                        hyperparameters)
    return actual_values, predictions, forecast_results, accuracy


forecast_pipeline('cotton')
