import pandas as pd
import sqlite3
import os
import zipfile
from airflow import DAG
import numpy as np
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi
from scipy import stats

# Default arguments for the DAG
default_args = {
    'owner': 'Le Thinh',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'Teamwork_etl_process',
    default_args=default_args,
    description='ETL pipeline for Weather Dataset with validation and trigger rules',
    schedule_interval='@daily',
)

# File paths
# csv_file_path = '/home/lethinh/airflow/datasets/2019.csv'
# db_path = '/home/lethinh/airflow/databases/happiness_data.db'

# Task 1: Extract data
def extract_data(**kwargs):
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download the dataset file
    api.dataset_download_file('muthuj7/weather-dataset', file_name='weatherHistory.csv', path='/home/lethinh/airflow/datasets')

    # Define file paths
    downloaded_file_path = '/home/lethinh/airflow/datasets/weatherHistory.csv'
    zip_file_path = downloaded_file_path + '.zip'

    # Check if the downloaded file is a ZIP file
    if zipfile.is_zipfile(zip_file_path):
        # If it's a ZIP file, unzip it
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('/home/lethinh/airflow/datasets')
        os.remove(zip_file_path)  # Optionally delete the ZIP file after extraction
    else:
        print("Downloaded file is not a ZIP archive, skipping extraction.")

    # Push the CSV file path to XCom for use in the next steps
    kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform data
def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

    # Convert a date to a right format
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)

    # Handle missing values in critical columns
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    for column in critical_columns:
        df[column].fillna(df[column].median(), inplace=True)

    # Remove duplicates
    df = df.drop_duplicates()

    # Daily data
    daily_data = df[['Formatted Date','Temperature (C)','Apparent Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']]

    # Get the date from Formatted Date 
    daily_data['Formatted Date'] = daily_data['Formatted Date'].dt.date

    # Classify wind category
    def wind(category):
        category = category * 0.277778  # Convert km/h to m/s
        if category <= 1.5:
            return 'Calm'
        elif category <= 3.3:
            return 'Light Air'
        elif category <= 5.4:
            return 'Light Breeze'
        elif category <= 7.9:
            return 'Gentle Breeze'
        elif category <= 10.7:
            return 'Moderate Breeze'
        elif category <= 13.8:
            return 'Fresh Breeze'
        elif category <= 17.1:
            return 'Strong Breeze'
        elif category <= 20.7:
            return 'Near Gale'
        elif category <= 24.4:
            return 'Gale'
        elif category <= 28.4:
            return 'Strong Gale'
        elif category <= 32.6:
            return 'Storm'
        else:
            return 'Violent Storm'

    # Calculate averages for daily data and reset indexes in order to get one row per day    
    daily_data = daily_data.groupby('Formatted Date')[['Temperature (C)', 'Apparent Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']].mean().reset_index()

    # Apply categorization
    daily_data['wind_strength'] = daily_data['Wind Speed (km/h)'].apply(wind)

    # Select needed columns from original dataframe for monthly_data
    monthly_data = df[['Formatted Date','Precip Type','Temperature (C)','Apparent Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']]

    # Create a month table 
    monthly_data['Month'] = monthly_data['Formatted Date'].dt.month

    # Calculate the mode of 'Precip Type' for each month
    mode_precip_type = monthly_data.groupby('Month')['Precip Type'].agg(lambda x: pd.Series.mode(x) if len(pd.Series.mode(x)) == 1 else np.nan).reset_index()

    # Merge the mode back into the original DataFrame
    monthly_data = monthly_data.merge(mode_precip_type.rename(columns={'Precip Type': 'Mode'}), on='Month', how='left')

    # Save the unique Month and Mode values in a separate DataFrame
    monthly_mode_month = monthly_data[['Month', 'Mode']].drop_duplicates()

    # Add column containing mode
    monthly_data = monthly_data.merge(monthly_mode_month, on='Month', how='left') 

    # Calculate mode of Precip Type for each month
    # Create new column for mode value/values
    #Mode = monthly_data.groupby('Month')['Precip Type'].agg(pd.Series.mode).rename('Mode')
    #monthly_data = monthly_data.merge(Mode, on='Month', how='left')
    # If there are more than one mode value per month, return NaN for this month
    #monthly_data['Mode'].transform(lambda x: np.nan if type(x) == list else x)
    # Save Month and its Mode in additional Dataframe in order to merge it later with averages
    #df_mode_month = monthly_data[['Month', 'Mode']]
    #df_mode_month = df_mode_month.drop_duplicates()

    # Calculate averages for each month
    monthly_data = monthly_data.groupby('Month')[['Temperature (C)', 'Apparent Temperature (C)', 'Humidity', 'Visibility (km)', 'Pressure (millibars)']].mean().reset_index()

    # Add column containing mode
    monthly_data = monthly_data.merge(monthly_mode_month, on='Month', how='left') 

    # Add column containing mode
    #monthly_data = monthly_data.merge(df_mode_month, on='Month', how='left')

    # Rename Month column with actual name of month based on it's number using function
    def month_name(mon_id):
        month_dict = {
            1: 'January',
            2: 'February',
            3: 'March',
            4: 'April',
            5: 'May',
            6: 'June',
            7: 'July',
            8: 'August',
            9: 'September',
            10: 'October',
            11: 'November',
            12: 'December'
        }
        return month_dict[mon_id]

    monthly_data['Month'] = monthly_data['Month'].apply(month_name)


    # Rename columns 
    # Daily data columns
    daily_data.rename(columns={
        'Formatted Date':'formatted_date',
        'Temperature (C)':'avg_temperature_c',
        'Apparent Temperature (C)':'avg_apparent_temperature_c',
        'Humidity':'avg_humidity', 
        'Wind Speed (km/h)':'avg_wind_speed_kmh', 
        'Visibility (km)':'avg_visibility_km', 
        'Pressure (millibars)':'avg_pressure_millibars'
        }, inplace=True)
    
    # Monthly data columns
    monthly_data.rename(columns={
        'Month':'month',
        'Temperature (C)':'avg_temperature_c',
        'Apparent Temperature (C)':'avg_apparent_temperature_c',
        'Humidity':'avg_humidity', 
        'Visibility (km)':'avg_visibility_km', 
        'Pressure (millibars)':'avg_pressure_millibars',
        'Mode':'mode_precip_type'
        }, inplace=True)

    # Save the transformed data to a new CSV files and pass file paths to XCom
    # For daily data
    transformed_daily_path = '/home/lethinh/airflow/datasets/transformed_daily_data.csv'
    daily_data.to_csv(transformed_daily_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_daily_path', value=transformed_daily_path)

    # For monthly data
    transformed_monthly_path = '/home/lethinh/airflow/datasets/transformed_monthly_data.csv'
    monthly_data.to_csv(transformed_monthly_path, index=False)
    kwargs['ti'].xcom_push(key='transformed_monthly_path', value=transformed_monthly_path)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Validate data
def validate_data(**kwargs):
    # Retrieve transformed file paths from XCom
    # Daily data
    transformed_daily_path = kwargs['ti'].xcom_pull(key='transformed_daily_path')
    daily_data = pd.read_csv(transformed_daily_path)
    # Monthly data
    transformed_monthly_path = kwargs['ti'].xcom_pull(key='transformed_monthly_path')
    monthly_data = pd.read_csv(transformed_monthly_path)

    # Ensure no critical columns have missing values after transformation
    # Daily data
    critical_columns_1 = ['avg_temperature_c', 'avg_humidity', 'avg_wind_speed_kmh']
    if daily_data[critical_columns_1].isnull().any().any():
        raise ValueError("Validation failed: Missing values in critical columns in daily data after transformation.")
    # Monthly data
    critical_columns_2 = ['avg_temperature_c', 'avg_humidity']
    if monthly_data[critical_columns_2].isnull().any().any():
        raise ValueError("Validation failed: Missing values in critical columns in monthly data after transformation.")

    # Range Check and outlier detection
    # For Temperature
    if not(daily_data['avg_temperature_c'].between(-50,50).all()):
        print(daily_data[~daily_data['avg_temperature_c'].between(-50,50)])
        raise ValueError("Validation failed: Unexpected range for temperature in daily data")
    if not(monthly_data['avg_temperature_c'].between(-50,50).all()):
        print(monthly_data[~monthly_data['avg_temperature_c'].between(-50,50)])
        raise ValueError("Validation failed: Unexpected range for temperature in monthly data")
    # For Humidity
    if not(daily_data['avg_humidity'].between(0,1).all()):
        print(monthly_data[~monthly_data['avg_humidity'].between(0,1)])
        raise ValueError("Validation failed: Unexpected range for humidity in daily data")
    if not(monthly_data['avg_humidity'].between(0,1).all()):
        print(monthly_data[~monthly_data['avg_humidity'].between(0,1)])
        raise ValueError("Validation failed: Unexpected range for humidity in monthly data")
    # For Wind Speed
    if not(daily_data['avg_wind_speed_kmh'].between(0,500).all()):
        print(daily_data[~daily_data['avg_wind_speed_kmh'].between(0,500)])
        raise ValueError("Validation failed: Unexpected range for wind speed in daily data")

    # Pass the file path to XCom for the next task
    kwargs['ti'].xcom_push(key='validated_daily_path', value=transformed_daily_path)
    kwargs['ti'].xcom_push(key='validated_monthly_path', value=transformed_monthly_path)

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)


# Task 4: Load data
def load_data(**kwargs):
    # Retrieve validated file path from XCom
    # Daily data
    validated_daily_path = kwargs['ti'].xcom_pull(key='validated_daily_path')
    daily_data = pd.read_csv(validated_daily_path)
    # Monthly data
    validated_monthly_path = kwargs['ti'].xcom_pull(key='validated_monthly_path')
    monthly_data = pd.read_csv(validated_monthly_path)

    # Load data into SQLite database
    db_path = '/home/lethinh/airflow/databases/weather_data_2.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop old daily data table if exists
    cursor.execute("""
        DROP TABLE IF EXISTS daily_weather;
    """)

    # Create new daily data table 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_weather (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
            "formatted_date" DATE,
            "avg_temperature_c" FLOAT,
            "avg_apparent_temperature_c" FLOAT,
            "avg_humidity" FLOAT,
            "avg_wind_speed_kmh" FLOAT,
            "avg_visibility_km" FLOAT,
            "avg_pressure_millibars" FLOAT,
            "wind_strength" VARCHAR(50)
        );
    """)

    # Drop old monthly data table
    cursor.execute("""
        DROP TABLE IF EXISTS monthly_weather;
    """)

    # Create new monthly data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS monthly_weather (
            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
            "month" VARCHAR(50),
            "avg_temperature_c" FLOAT,
            "avg_apparent_temperature_c" FLOAT,
            "avg_humidity" FLOAT,
            "avg_visibility_km" FLOAT,
            "avg_pressure_millibars" FLOAT,
            "mode_precip_type" VARCHAR(50)
        );
    """)

    # Insert actual data into the database to corresponding tables
    daily_data.to_sql('daily_weather', conn, if_exists='append', index=False)
    monthly_data.to_sql('monthly_weather', conn, if_exists='append', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',  # Ensures load_task only runs if validate_task is successful
    dag=dag,
)

extract_task >> transform_task >> validate_task >> load_task