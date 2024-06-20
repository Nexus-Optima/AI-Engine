import json
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
from DL_Models.LSTM.lstm_tuning import tune_lstm_hyperparameters
from Constants.constants import Credentials
import boto3

s3 = boto3.client('s3', aws_access_key_id=Credentials.aws_access_key_id,
                  aws_secret_access_key=Credentials.aws_secret_access_key)

load_dotenv()
# Boto3 clients

#make it generic for xgboost and other models as well

commodity_name = "cotton"  # Replace with your actual commodity name
S3_UPLOAD_BUCKET_NAME = 'b3llcurve-hyperparameters'
def tuning(commodity_name):
    for forecasting_week in prms.FORECASTING_WEEKS:

        output_key = f'parameter/{commodity_name}/lstm_parameters_4Y_{forecasting_week}W'

        read_df_weekly = read_data_s3(cts.Commodities.COMMODITIES, commodity_name)

        processed_data_weekly = process_data_weekly(read_df_weekly)

        processed_data_weekly_lagged = process_data_lagged(processed_data_weekly, forecasting_week)

        features_dataset_weekly = create_features_dataset(processed_data_weekly_lagged.copy())

        features_dataset_weekly = features_dataset_weekly.last('4Y')

        # here only lstm tuning is happening make it generic for all models
        params = tune_lstm_hyperparameters(features_dataset_weekly, 1)

        params_json = json.dumps(params)

        print(f"Running tuning for forecasting_days = {forecasting_week}")
        print(params_json)

        upload_data_to_s3(params_json, S3_UPLOAD_BUCKET_NAME, output_key)




    for forecasting_day in prms.FORECASTING_DAYS:

        output_key = f'parameter/{commodity_name}/lstm_parameters_4Y_{forecasting_day}D'

        read_df = read_data_s3(cts.Commodities.COMMODITIES, commodity_name)

        processed_data = process_data_lagged(read_df, forecasting_day)

        features_dataset = create_features_dataset(processed_data.copy())

        features_dataset = features_dataset.last('4Y')

        params = tune_lstm_hyperparameters(features_dataset, 1)

        params_json = json.dumps(params)

        print(f"Running tuning for forecasting_days = {forecasting_day}")
        print(params_json)

        upload_data_to_s3(params_json, S3_UPLOAD_BUCKET_NAME, output_key)


def upload_data_to_s3(data, bucket_name, s3_key):
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        print(f"Data uploaded successfully to S3: {s3_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")


# tuning('cotton')
