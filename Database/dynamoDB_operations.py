import boto3
import pandas as pd
import json
from decimal import Decimal
from Constants.constants import Credentials

dynamodb = boto3.resource("dynamodb",
                          region_name="ap-south-1",
                          aws_access_key_id=Credentials.aws_access_key_id,
                          aws_secret_access_key=Credentials.aws_secret_access_key
                          )

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


def store_forecast(bucket_name, folder_name, datasets):
    """
    Stores multiple datasets in S3 as JSON files.

    Parameters:
    - bucket_name: Name of the S3 bucket.
    - folder_name: Folder path within the S3 bucket.
    - datasets: A dictionary where keys are filenames and values are pandas DataFrames.
    """
    try:
        s3_client = boto3.client('s3', aws_access_key_id=Credentials.aws_access_key_id,
                                 aws_secret_access_key=Credentials.aws_secret_access_key)

        for filename, df in datasets.items():
            json_data = df.to_json(orient='records', date_format='iso')
            file_path = f"{folder_name}/{filename}.json"
            s3_client.put_object(Bucket=bucket_name, Key=file_path, Body=json_data)
            print(f"Uploaded {filename} to s3://{bucket_name}/{file_path}")
    except Exception as e:
        raise Exception(f"Failed to store forecast: {e}")