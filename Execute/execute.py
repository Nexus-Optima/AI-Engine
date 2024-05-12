import os
import uuid
import json
from datetime import datetime
import boto3
import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Importing necessary constants and parameters.
from Constants.constants import Credentials, Commodities
import Constants.constants as cts
import Constants.parameters as prms
from Data_Preprocessing.data_processing import create_features_dataset
from Database.s3_operations import read_data_s3

# Utilities for data processing.
from Utils.process_data import process_data_lagged

from decimal import Decimal

# Importing different modeling approaches.
from Models.XG_Boost.adaptive_xgboost import execute_adaptive_xgboost
# from Models.LightGBM.LGBM import execute_LGBM
from Models.ETS import execute_ets
from Models.SARIMAX import execute_sarimax
from Models.Arima import execute_arima
from DL_Models.LSTM.LSTM import execute_lstm

load_dotenv()

# Boto3 clients
dynamodb = boto3.resource("dynamodb",
                          region_name="ap-south-1",
                          aws_access_key_id=Credentials.aws_access_key_id,
                          aws_secret_access_key=Credentials.aws_secret_access_key
                          )
s3 = boto3.client('s3', aws_access_key_id=Credentials.aws_access_key_id,
                  aws_secret_access_key=Credentials.aws_secret_access_key)

# DynamoDB Table Name and S3 Bucket Name
DYNAMODB_TABLE_NAME = 'model-details'
S3_BUCKET_NAME = 'b3ll-curve-model-storage'


def store_model_details_in_dynamoDB(model_name, accuracy, hyper_parameters, input_columns, s3_path):
    """Store or update model details in DynamoDB with proper data serialization."""
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    # Convert hyper_parameters if it's a Series or DataFrame
    if isinstance(hyper_parameters, pd.Series):
        hyper_parameters = hyper_parameters.to_dict()
    hyper_parameters = json.dumps(hyper_parameters)  # Serialize for DynamoDB

    # Ensure input_columns is a list
    if not isinstance(input_columns, list):
        input_columns = list(input_columns)

    accuracy = Decimal(str(accuracy)) if accuracy is not None else None

    try:
        table.put_item(
            Item={
                'ModelID': model_name,
                'Accuracy': accuracy,
                'HyperParameters': hyper_parameters,
                'InputColumns': input_columns,
                'S3Path': f's3://{S3_BUCKET_NAME}/{s3_path}'
            }
        )
        print(f"Stored or updated model details for {model_name} in DynamoDB.")
    except Exception as e:
        print(f"Failed to store or update model details in DynamoDB: {e}")


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
        predictions, forecast_outputs, accuracy = execute_model(
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

    # Store all details in a single S3 file
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_path, Body=json.dumps(all_model_details))


def execute_model(model_func, raw_data, processed_data, forecast, hyperparameters):
    """
    General function to execute any model with the given data, forecast period, and hyperparameters.
    Assumes model_func is a callable that matches this signature.
    """
    predictions, forecast_results, accuracy = model_func(raw_data, processed_data, forecast, hyperparameters)
    print(len(predictions))
    return predictions, forecast_results, accuracy
