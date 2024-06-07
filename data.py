import pandas as pd
import os
import shutil
def clean_data(df, date_format):
    df.columns = df.columns.str.strip()
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], format=date_format, errors='coerce')
        df.dropna(subset=['Date'], inplace=True)
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

    if 'Volume' in df.columns:
        df['Volume'] = df['Volume'].apply(lambda x: x.replace('K', '') if isinstance(x, str) else x).astype(float)

    if 'Chg%' in df.columns:
        df['Chg%'] = df['Chg%'].apply(lambda x: x.replace('%', '') if isinstance(x, str) else x).astype(float).round(2)

    for col in ['Price', 'Open', 'High', 'Low', 'Value','Shankar 6']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.replace(',', '') if isinstance(x, str) and ',' in x else x).astype(float)

    return df


def left_join_and_adjust_prices(left_df, right_df, columns, join_type='left'):
    if not all(col in left_df.columns for col in columns):
        missing_cols = [col for col in columns if col not in left_df.columns]
        raise KeyError(f"Missing columns in DataFrame: {missing_cols}")

    merged_df = pd.merge(left_df, right_df, on='Date', how=join_type, suffixes=('_left', '_right'))
    for col in columns[1:]:
        merged_df[col] = merged_df[f'{col}_left'] * merged_df[f'{col}_right']
        merged_df.drop(columns=[f'{col}_left', f'{col}_right'], inplace=True)

    merged_df.sort_values(by='Date', inplace=True)
    if 'Volume' in left_df.columns:
        merged_df['Volume'] = left_df['Volume']

    if 'Chg%' in left_df.columns:
        merged_df['Chg%'] = left_df['Chg%']

    return merged_df

def append_to_s3(existing_file, new_data):
    if os.path.isfile(existing_file):
        existing_df = pd.read_csv(existing_file)
    else:
        existing_df = pd.DataFrame()

    new_data_df = pd.read_csv(new_data)
    merged_df = pd.concat([existing_df, new_data_df], ignore_index=True)
    merged_df.to_csv(existing_file, index=False)

file_paths = {
    'china': ['./Data/China.csv', './Data/China_convert.csv'],
    'usa': ['./Data/USA.csv', './Data/Usa_convert.csv'],
    'brazil': ['./Data/Brazil.csv', './Data/Convert.csv'],
    'cotlook': ['./Data/Cotlook_data.csv', './Data/Convert.csv'],
    'spot_price': ['./Data/Spot_price_data.csv'],
    'india': ['./Data/India.csv']
}
s3_paths = {
    'china': './Data_S3/China Cotton Futures.csv',
    'usa': './Data_S3/US Cotton Futures.csv',
    'brazil': './Data_S3/Brazil Cotton Index.csv',
    'cotlook': './Data_S3/Cotlook.csv',
    'spot_price': './Data_S3/Spot Prices.csv',
    'india': './Data_S3/IND Cotton Futures.csv'
}
column_mappings = {
    'china': {
        'Price': 'China_Cotton_FT_Price',
        'Open': 'China_Cotton_FT_Open',
        'High': 'China_Cotton_FT_High',
        'Low': 'China_Cotton_FT_Low',
        'Volume': 'China_Cotton_FT_Volume',
        'Chg%': 'China_Cotton_FT_Chg'
    },
    'usa': {
        'Price': 'US_Cotton_FT_Price',
        'Open': 'US_Cotton_FT_Open',
        'High': 'US_Cotton_FT_High',
        'Low': 'US_Cotton_FT_Low',
        'Volume': 'US_Cotton_FT_Volume',
        'Chg%': 'US_Cotton_FT_Chg'
    },
    'brazil': {
        'Value': 'Value_Brazil_Index'
    },
    'cotlook': {
        'Value': 'Cotlook_A_index'
    },
    'spot_price': {
        'Shankar 6': 'Output'
    },
    'india': {
        'Price': 'Ind_Cotton_FT_Price',
        'Open': 'Ind_Cotton_FT_Open',
        'High': 'Ind_Cotton_FT_High',
        'Low': 'Ind_Cotton_FT_Low',
        'Volume': 'Ind_Cotton_FT_Volume',
        'Chg%': 'Ind_Cotton_FT_Chg'
    }
}

for name, paths in file_paths.items():
    df = pd.read_csv(paths[0])
    if 'Date' not in df.columns:
        raise ValueError(f"'Date' column not found in {name} DataFrame.")

    if name in ['brazil', 'cotlook', 'spot_price']:
        df = clean_data(df, date_format='%d-%m-%Y')
    else:
        df = clean_data(df, date_format='%b %d, %Y')

    if name in column_mappings and df.columns[0] in column_mappings[name]:
        df.rename(columns=column_mappings[name], inplace=True)

    if name in ['china']:
        right_df = pd.read_csv(paths[1])
        right_df = clean_data(right_df, date_format='%b %d, %Y')

        try:
            merged_df = left_join_and_adjust_prices(df, right_df, ['Date', 'Price', 'Open', 'High', 'Low'],join_type='left')
            merged_df.rename(columns=column_mappings[name], inplace=True)
            merged_df[['Date','China_Cotton_FT_Price','China_Cotton_FT_Open','China_Cotton_FT_High','China_Cotton_FT_Low','China_Cotton_FT_Volume','China_Cotton_FT_Chg']].to_csv(
                f'./Data/{name.capitalize()}_merged.csv', index=False, float_format='%.2f')
            append_to_s3('./Data_S3/China Cotton Futures.csv', f'./Data/China_merged.csv')
        except KeyError as e:
            print(f"Skipping {name} due to missing columns: {e}")

    elif name in ['usa']:
        right_df = pd.read_csv(paths[1])
        right_df = clean_data(right_df, date_format='%b %d, %Y')

        try:
            merged_df = left_join_and_adjust_prices(df, right_df, ['Date', 'Price', 'Open', 'High', 'Low'],
                                                    join_type='left')
            merged_df.rename(columns=column_mappings[name], inplace=True)
            merged_df[['Date','US_Cotton_FT_Price','US_Cotton_FT_Open','US_Cotton_FT_High','US_Cotton_FT_Low','US_Cotton_FT_Volume','US_Cotton_FT_Chg']].to_csv(
                f'./Data/{name.capitalize()}_merged.csv', index=False, float_format='%.2f')
            append_to_s3('./Data_S3/US Cotton Futures.csv', f'./Data/Usa_merged.csv')
        except KeyError as e:
            print(f"Skipping {name} due to missing columns: {e}")

    elif name in ['brazil']:
        right_df = pd.read_csv(paths[1])
        right_df = clean_data(right_df, date_format='%B %d, %Y')

        try:
            merged_df = left_join_and_adjust_prices(df, right_df, ['Date', 'Value'], join_type='inner')
            merged_df.rename(columns=column_mappings[name], inplace=True)
            merged_df[['Date','Value_Brazil_Index']].to_csv(f'./Data/{name.capitalize()}_merged.csv', index=False,
                                                float_format='%.2f')
            append_to_s3('./Data_S3/Brazil Cotton Index.csv', f'./Data/Brazil_merged.csv')
        except KeyError as e:
            print(f"Skipping {name} due to missing columns: {e}")

    elif name in ['cotlook']:
        right_df = pd.read_csv(paths[1])
        right_df = clean_data(right_df, date_format='%B %d, %Y')

        try:
            merged_df = left_join_and_adjust_prices(df, right_df, ['Date', 'Value'], join_type='inner')
            merged_df.rename(columns=column_mappings[name], inplace=True)
            merged_df[['Date','Cotlook_A_index']].to_csv(f'./Data/{name.capitalize()}_merged.csv', index=False,
                             float_format='%.2f')
            append_to_s3('./Data_S3/Cotlook.csv', f'./Data/Cotlook_merged.csv')
        except KeyError as e:
            print(f"Skipping {name} due to missing columns: {e}")

    elif name in ['spot_price']:
        df.rename(columns={'Shankar 6': 'Output'}, inplace=True)
        df.sort_values(by='Date', inplace=True)
        df.to_csv(f'./Data/{name.capitalize()}_cleaned.csv', index=False, float_format='%.2f')
        append_to_s3('./Data_S3/Spot Prices.csv', f'./Data/Spot_price_cleaned.csv')

    else:
        df.sort_values(by='Date', inplace=True)
        df.rename(columns=column_mappings[name], inplace=True)
        df.to_csv(f'./Data/{name.capitalize()}_cleaned.csv', index=False, float_format='%.2f')
        append_to_s3('./Data_S3/IND Cotton Futures.csv', f'./Data/India_cleaned.csv')
