import pandas as pd

def clean_data(df, date_format):
    df.columns = df.columns.str.strip()
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], format=date_format, errors='coerce')
        df.dropna(subset=['Date'], inplace=True)
        df['Date'] = df['Date'].dt.strftime('%d/%m/%y')

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

    if 'Volume' in left_df.columns:
        merged_df['Volume'] = left_df['Volume']

    if 'Chg%' in left_df.columns:
        merged_df['Chg%'] = left_df['Chg%']

    return merged_df