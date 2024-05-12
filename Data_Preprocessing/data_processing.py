import Constants.constants as cts
import pandas as pd


def create_features_dataset(processed_data):
    """Function to create a dataset using selected features based on correlation methods."""
    common_selected_features = set(processed_data.columns)
    for method_name in cts.correlation_methods:
        selected_features_df = method_name(processed_data)
        common_selected_features = common_selected_features.intersection(set(selected_features_df.columns))
    common_selected_features.discard('Output')
    features_dataset = processed_data[['Output'] + list(common_selected_features)]

    return features_dataset


def standardize_dataset(df, date_column, num_columns):
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    for col in num_columns:
        if df[col].dtype == object:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')

    return df

