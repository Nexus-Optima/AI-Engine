from Database.s3_operations import read_raw_data_from_s3, upload_converted_data_to_s3
from Utils.Data_operations import clean_data, left_join_and_adjust_prices
import Constants.constants as cts
import pandas as pd
import io

S3_UPLOAD_BUCKET_NAME =cts.Commodities.COMMODITIES
S3_READ_BUCKET_NAME = 'b3llcurve-rawdata'

def brazil_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/brazil/Brazil.csv'
    convert_key = f'{commodity_name}/Data/Conversion/Convert.csv'
    output_key = f'{commodity_name}/Brazil Cotton Index.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    convert_df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, convert_key)

    df = clean_data(df, date_format='%d-%m-%Y')
    convert_df = clean_data(convert_df, date_format='%B %d, %Y')

    try:
        merged_df = left_join_and_adjust_prices(df, convert_df, ['Date', 'Value'], join_type='inner')
        merged_df.rename(columns={'Value': 'Value_Brazil_Index'}, inplace=True)
        existing_df = read_raw_data_from_s3(S3_UPLOAD_BUCKET_NAME, output_key)
        updated_df = pd.concat([existing_df, merged_df[['Date', 'Value_Brazil_Index']]]).drop_duplicates(subset='Date', keep='last')
        updated_csv_buffer = io.StringIO()
        updated_df.to_csv(updated_csv_buffer, index=False, float_format='%.2f')
        upload_converted_data_to_s3(updated_csv_buffer.getvalue(), S3_UPLOAD_BUCKET_NAME, output_key)
    except KeyError as e:
        print(f"Skipping Brazil due to missing columns: {e}")

def cotlook_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/Cotlook/Cotlook_data.csv'
    convert_key = f'{commodity_name}/Data/Conversion/Convert.csv'
    output_key = f'{commodity_name}/Cotlook.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    convert_df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, convert_key)

    df = clean_data(df, date_format='%d-%m-%Y')
    convert_df = clean_data(convert_df, date_format='%B %d, %Y')

    try:
        merged_df = left_join_and_adjust_prices(df, convert_df, ['Date', 'Value'], join_type='inner')
        merged_df.rename(columns={'Value': 'Cotlook_A_index'}, inplace=True)
        existing_df = read_raw_data_from_s3(S3_UPLOAD_BUCKET_NAME, output_key)
        updated_df = pd.concat([existing_df, merged_df[['Date', 'Cotlook_A_index']]], ignore_index=True).drop_duplicates(subset='Date', keep='last')
        updated_csv_buffer = io.StringIO()
        updated_df.to_csv(updated_csv_buffer, index=False, float_format='%.2f')
        upload_converted_data_to_s3(updated_csv_buffer.getvalue(), S3_UPLOAD_BUCKET_NAME, output_key)
    except KeyError as e:
        print(f"Skipping Cotlook due to missing columns: {e}")

def china_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/China/China.csv'
    convert_key = f'{commodity_name}/Data/China/China_convert.csv'
    output_key = f'{commodity_name}/China Cotton Futures.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    convert_df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, convert_key)

    df = clean_data(df, date_format='%b %d, %Y')
    convert_df = clean_data(convert_df, date_format='%b %d, %Y')

    try:
        merged_df = left_join_and_adjust_prices(df, convert_df, ['Date', 'Price', 'Open', 'High', 'Low'], join_type='left')
        merged_df.rename(columns={
            'Price': 'China_Cotton_FT_Price',
            'Open': 'China_Cotton_FT_Open',
            'High': 'China_Cotton_FT_High',
            'Low': 'China_Cotton_FT_Low',
            'Volume': 'China_Cotton_FT_Volume',
            'Chg%': 'China_Cotton_FT_Chg'
        }, inplace=True)
        existing_df = read_raw_data_from_s3(S3_UPLOAD_BUCKET_NAME, output_key)
        updated_df = pd.concat([existing_df, merged_df[['Date', 'China_Cotton_FT_Price', 'China_Cotton_FT_Open', 'China_Cotton_FT_High', 'China_Cotton_FT_Low', 'China_Cotton_FT_Volume', 'China_Cotton_FT_Chg']]], ignore_index=True).drop_duplicates(subset='Date', keep='last')
        updated_csv_buffer = io.StringIO()
        updated_df.to_csv(updated_csv_buffer, index=False, float_format='%.2f')
        upload_converted_data_to_s3(updated_csv_buffer.getvalue(), S3_UPLOAD_BUCKET_NAME, output_key)
    except KeyError as e:
        print(f"Skipping China due to missing columns: {e}")

def usa_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/USA/Usa.csv'
    convert_key = f'{commodity_name}/Data/USA/Usa_convert.csv'
    output_key = f'{commodity_name}/US Cotton Futures.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    convert_df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, convert_key)

    df = clean_data(df, date_format='%b %d, %Y')
    convert_df = clean_data(convert_df, date_format='%b %d, %Y')

    try:
        merged_df = left_join_and_adjust_prices(df, convert_df, ['Date', 'Price', 'Open', 'High', 'Low'], join_type='left')
        merged_df.rename(columns={
            'Price': 'US_Cotton_FT_Price',
            'Open': 'US_Cotton_FT_Open',
            'High': 'US_Cotton_FT_High',
            'Low': 'US_Cotton_FT_Low',
            'Volume': 'US_Cotton_FT_Volume',
            'Chg%': 'US_Cotton_FT_Chg'
        }, inplace=True)
        existing_df = read_raw_data_from_s3(S3_UPLOAD_BUCKET_NAME, output_key)
        updated_df = pd.concat([existing_df, merged_df[['Date', 'US_Cotton_FT_Price', 'US_Cotton_FT_Open', 'US_Cotton_FT_High', 'US_Cotton_FT_Low', 'US_Cotton_FT_Volume', 'US_Cotton_FT_Chg']]], ignore_index=True).drop_duplicates(subset='Date', keep='last')
        updated_csv_buffer = io.StringIO()
        updated_df.to_csv(updated_csv_buffer, index=False, float_format='%.2f')
        upload_converted_data_to_s3(updated_csv_buffer.getvalue(), S3_UPLOAD_BUCKET_NAME, output_key)
    except KeyError as e:
        print(f"Skipping Usa due to missing columns: {e}")

def india_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/India/India.csv'
    output_key = f'{commodity_name}/IND Cotton Futures.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    df = clean_data(df, date_format='%b %d, %Y')

    try:
        df.sort_values(by='Date', inplace=True)
        df.rename(columns={
            'Price': 'Ind_Cotton_FT_Price',
            'Open': 'Ind_Cotton_FT_Open',
            'High': 'Ind_Cotton_FT_High',
            'Low': 'Ind_Cotton_FT_Low',
            'Volume': 'Ind_Cotton_FT_Volume',
            'Chg%': 'Ind_Cotton_FT_Chg'
        }, inplace=True)
        existing_df = read_raw_data_from_s3(S3_UPLOAD_BUCKET_NAME, output_key)
        updated_df = pd.concat([existing_df, df[['Date', 'Ind_Cotton_FT_Price', 'Ind_Cotton_FT_Open', 'Ind_Cotton_FT_High', 'Ind_Cotton_FT_Low', 'Ind_Cotton_FT_Volume', 'Ind_Cotton_FT_Chg']]], ignore_index=True).drop_duplicates(subset='Date', keep='last')
        updated_csv_buffer = io.StringIO()
        updated_df.to_csv(updated_csv_buffer, index=False, float_format='%.2f')
        upload_converted_data_to_s3(updated_csv_buffer.getvalue(), S3_UPLOAD_BUCKET_NAME, output_key)
    except KeyError as e:
        print(f"Skipping India due to missing columns: {e}")

def spot_price_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/Spot_Prices/Spot_price_data.csv'
    output_key = f'{commodity_name}/Spot Prices.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    df = clean_data(df, date_format='%d-%m-%Y')

    try:
        df.rename(columns={'Shankar 6': 'Output'}, inplace=True)
        existing_df = read_raw_data_from_s3(S3_UPLOAD_BUCKET_NAME, output_key)
        updated_df = pd.concat([existing_df, df[['Date', 'Output']]], ignore_index=True).drop_duplicates(subset='Date', keep='last')
        updated_csv_buffer = io.StringIO()
        updated_df.to_csv(updated_csv_buffer, index=False, float_format='%.2f')
        upload_converted_data_to_s3(updated_csv_buffer.getvalue(), S3_UPLOAD_BUCKET_NAME, output_key)
    except KeyError as e:
        print(f"Skipping spot price due to missing columns: {e}")

def data_conversion(commodity_name):
    brazil_conversion(commodity_name)
    china_conversion(commodity_name)
    usa_conversion(commodity_name)
    india_conversion(commodity_name)
    spot_price_conversion(commodity_name)
    cotlook_conversion(commodity_name)

data_conversion('cotton')
