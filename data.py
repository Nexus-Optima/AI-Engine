from Database.s3_operations import read_raw_data_from_s3, upload_converted_data_to_s3
from Utils.Data_operations import clean_data, left_join_and_adjust_prices
from Constants.url import Urls
import Constants.constants as cts
import pandas as pd
import io
import boto3
from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
from datetime import datetime,timedelta
import os

app = Flask(__name__)

os.environ['AWS_ACCESS_KEY_ID'] =Credentials.aws_access_key_id
os.environ['AWS_SECRET_ACCESS_KEY'] =Credentials.aws_secret_access_key
S3_UPLOAD_BUCKET_NAME =cts.Commodities.COMMODITIES
S3_READ_BUCKET_NAME = 'b3llcurve-rawdata'


def fetch_data():
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        commodity_name = 'cotton'

        china_data = fetch_data_china(start_date, end_date)
        china_convert_data = fetch_data_china_convert(start_date, end_date)
        save_data_to_s3(commodity_name, china_data, "China", "China")
        save_data_to_s3(commodity_name, china_convert_data, "China", "China_convert")


        usa_data = fetch_data_usa(start_date, end_date)
        usa_convert_data = fetch_data_usa_convert(start_date, end_date)
        save_data_to_s3(commodity_name, usa_data, "USA", "Usa")
        save_data_to_s3(commodity_name, usa_convert_data, "USA", "Usa_convert")

        india_data = fetch_data_india(start_date, end_date)
        save_data_to_s3(commodity_name, india_data, "India", "India")

        brazil_data = fetch_data_brazil(start_date, end_date)
        brazil_convert_data = fetch_data_brazil_convert(start_date, end_date)
        save_data_to_s3(commodity_name, brazil_data, "brazil", "Brazil")
        save_data_to_s3(commodity_name, brazil_convert_data, "Conversion", "Convert")

        spot_price_data = fetch_data_spot_price(start_date, end_date)
        save_data_to_s3(commodity_name, spot_price_data, "Spot_Prices", "Spot_price_data")

        data_conversion(commodity_name)

        return jsonify({"message": "Data fetched, stored, and processed successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def fetch_historical_data(url, start_date, end_date):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='freeze-column-w-1 w-full overflow-x-auto text-xs leading-4')
    if not table:
        raise Exception("No historical data table found on the page")

    table_rows = table.find_all('tr')

    if not table_rows:
        raise Exception("No table rows found in the historical data table")

    data = []
    for row in table_rows[1:]:
        columns = row.find_all('td')
        if len(columns) == 7:
            date_str = columns[0].text.strip()
            date = datetime.strptime(date_str, '%m/%d/%Y')
            price = float(columns[1].text.replace(',', ''))
            open_ = float(columns[2].text.replace(',', ''))
            high = float(columns[3].text.replace(',', ''))
            low = float(columns[4].text.replace(',', ''))
            vol = columns[5].text.strip()
            change = columns[6].text.strip()

            if start_date <= date <= end_date:
                data.append([date, price, open_, high, low, vol, change])

    if not data:
        raise Exception("No data points found within the specified date range")

    df = pd.DataFrame(data, columns=['Date', 'Price', 'Open', 'High', 'Low', 'Volume', 'Chg%'])
    return df

def fetch_data_china(start_date, end_date):
    url = Urls.CHINA
    return fetch_historical_data(url, start_date, end_date)

def fetch_data_china_convert(start_date, end_date):
    url = Urls.CHINA_CONVERT
    return fetch_historical_data(url, start_date, end_date)

def fetch_data_usa(start_date, end_date):
    url = Urls.USA
    return fetch_historical_data(url, start_date, end_date)

def fetch_data_usa_convert(start_date, end_date):
    url = Urls.USA_CONVERT
    return fetch_historical_data(url, start_date, end_date)

def fetch_data_india(start_date, end_date):
    url = Urls.INDIA
    return fetch_historical_data(url, start_date, end_date)
def fetch_data_spot_price(start_date, end_date):
    url = Urls.SPOT_PRICE
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='table table-bordered text-right')
    if not table:
        raise Exception("No historical data table found on the page")

    table_rows = table.find_all('tr')

    if not table_rows:
        raise Exception("No table rows found in the historical data table")

    data = []
    for row in table_rows[1:]:
        columns = row.find_all('td')
        if len(columns) >= 2:
            date_str = columns[0].text.strip()
            date = datetime.strptime(date_str, '%d/%m/%Y')
            shankar_6 = float(columns[1].text.replace(',', ''))


            if start_date <= date <= end_date:
                data.append([date, shankar_6 ])

    if not data:
        raise Exception("No data points found within the specified date range")

    df = pd.DataFrame(data, columns=['Date', 'Shankar 6 29-3.8-76'])
    return df


def fetch_data_brazil(start_date, end_date):
    url = Urls.BRAZIL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from {url}, status code: {response.status_code}")

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='imagenet-table imagenet-th2 imagenet-col-12')
    if not table:
        raise Exception("No historical data table found on the page")

    table_rows = table.find_all('tr')

    if not table_rows:
        raise Exception("No table rows found in the historical data table")

    data = []
    for row in table_rows[1:]:
        columns = row.find_all('td')
        if len(columns) >= 1:
            date_str = columns[0].text.strip()
            date = datetime.strptime(date_str, '%m/%d/%Y')
            value = float(columns[1].text.replace(',', ''))

            if start_date <= date <= end_date:
                data.append([date, value])

    if not data:
        raise Exception("No data points found within the specified date range")

    df = pd.DataFrame(data, columns=['Date', 'Value'])
    return df

def fetch_data_brazil_convert(start_date, end_date):
    url = Urls.BRAZIL_CONVERT
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', class_='freeze-column-w-1 w-full overflow-x-auto text-xs leading-4')
    if not table:
        raise Exception("No historical data table found on the page")

    table_rows = table.find_all('tr')

    if not table_rows:
        raise Exception("No table rows found in the historical data table")

    data = []
    for row in table_rows[1:]:
        columns = row.find_all('td')
        if len(columns) >= 1:
            date_str = columns[0].text.strip()
            date = datetime.strptime(date_str, '%m/%d/%Y')
            price = float(columns[1].text.replace(',', ''))

            if start_date <= date <= end_date:
                data.append([date, price])

    if not data:
        raise Exception("No data points found within the specified date range")

    df = pd.DataFrame(data, columns=['Date', 'Price'])
    return df

def save_data_to_s3(commodity_name, df, country, data_type):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_key = f'{commodity_name}/Data/{country}/{data_type}.csv'
    s3_resource.Object(S3_READ_BUCKET_NAME, s3_key).put(Body=csv_buffer.getvalue())


def brazil_conversion(commodity_name):
    data_key = f'{commodity_name}/Data/brazil/Brazil.csv'
    convert_key = f'{commodity_name}/Data/Conversion/Convert.csv'
    output_key = f'{commodity_name}/Brazil Cotton Index.csv'

    df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, data_key)
    convert_df = read_raw_data_from_s3(S3_READ_BUCKET_NAME, convert_key)

    df = clean_data(df, date_format='%Y-%m-%d')
    convert_df = clean_data(convert_df, date_format='%Y-%m-%d')

    try:
        df.rename(columns={'Value': 'Price'}, inplace=True)
        merged_df = left_join_and_adjust_prices(df, convert_df, ['Date', 'Price'], join_type='inner')
        merged_df.rename(columns={'Price': 'Value_Brazil_Index'}, inplace=True)
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
    convert_df = clean_data(convert_df, date_format='%Y-%m-%d')

    try:
        convert_df.rename(columns={'Price': 'Value'}, inplace=True)
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

    df = clean_data(df, date_format='%Y-%m-%d')
    convert_df = clean_data(convert_df, date_format='%Y-%m-%d')

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

    df = clean_data(df, date_format='%Y-%m-%d')
    convert_df = clean_data(convert_df, date_format='%Y-%m-%d')

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
    df = clean_data(df, date_format='%Y-%m-%d')
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
    df = clean_data(df, date_format='%Y-%m-%d')

    try:
        df.rename(columns={'Shankar 6 29-3.8-76': 'Output'}, inplace=True)
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

