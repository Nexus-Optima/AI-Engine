import pandas as pd
from tsfresh import extract_features, extract_relevant_features, select_features
from Utils.process_data import process_data
from tsfresh.utilities.dataframe_functions import impute


def main():
    data = pd.read_csv('../Data/ICAC multiple variables.csv')
    data = process_data(data)
    data.reset_index(inplace=True)
    data['id'] = 0
    exclude_columns = ['Date', 'Output', 'Year', 'Month', 'Day', 'Season']
    data_for_extraction = data.drop(columns=exclude_columns)
    long_format_data = pd.melt(data, id_vars=['id', 'Date'], value_vars=data_for_extraction.columns,
                               var_name='variable', value_name='value')

    print(long_format_data)
    extracted_features = extract_features(long_format_data, column_id="id", column_sort="Date", column_value="value",
                                          column_kind="variable")

    imputed_features = impute(extracted_features)
    print(imputed_features.head())


if __name__ == '__main__':
    main()
