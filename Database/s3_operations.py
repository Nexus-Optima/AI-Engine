from Data_Preprocessing.data_processing import standardize_dataset
from Constants.constants import Credentials

import pandas as pd
import io
import boto3

s3 = boto3.client('s3',aws_access_key_id=Credentials.aws_access_key_id,
                  aws_secret_access_key=Credentials.aws_secret_access_key)
S3_BUCKET_NAME = 'b3ll-curve-model-storage'

def read_data_s3(bucket_name, folder_name):
    """Function to read and process data from an S3 bucket folder.

    Parameters:
    - bucket_name: Name of the S3 bucket.
    - folder_name: Folder path within the S3 bucket.
    """

    try:
        def custom_date_parser(date_string):
            return pd.to_datetime(date_string, format='%d/%m/%y')

        # List files in the specified folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        files = [item['Key'] for item in response['Contents'] if item['Key'].endswith('.csv')]

        all_data = []
        for file_key in files:
            csv_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            body = csv_obj['Body']
            data = pd.read_csv(io.BytesIO(body.read()), parse_dates=['Date'], date_parser=custom_date_parser)
            all_data.append(data)

        # print(all_data)
        standardized_datasets = []

        for df in all_data:
            standardized_df = standardize_dataset(df, 'Date', df.columns.drop('Date'))
            standardized_datasets.append(standardized_df)

        all_dates = pd.date_range(start=min(df['Date'].min() for df in standardized_datasets),
                                  end=max(df['Date'].max() for df in standardized_datasets))

        date_df = pd.DataFrame(all_dates, columns=['Date'])
        for df in standardized_datasets:
            date_df = pd.merge(date_df, df, on='Date', how='left')

        date_column = date_df['Date']
        date_df['Date'] = pd.to_datetime(date_df['Date'])
        date_df = date_df[date_df['Date'].dt.dayofweek < 5]
        date_df.drop('Date', axis=1, inplace=True)
        date_df.interpolate(method='linear', inplace=True)
        date_df['Date'] = date_column
        date_df = date_df[['Date'] + [col for col in date_df.columns if col != 'Date']]

        return date_df
    except Exception as e:
        raise Exception(f"Failed to read data from S3: {e}")


def store_models_s3(s3_path, body):
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_path, Body=body)

def read_raw_data_from_s3(bucket_name, s3_key):
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_csv(obj['Body'])
        return df
    except Exception as e:
        print(f"Error reading data from S3: {e}")
        return None

def upload_converted_data_to_s3(data, bucket_name, s3_key):
    try:
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=data)
        print(f"Data uploaded successfully to S3: {s3_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")
