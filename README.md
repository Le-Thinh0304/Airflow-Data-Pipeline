Weather ETL Pipeline with Airflow
Overview

This repository contains an ETL (Extract, Transform, Load) pipeline implemented with Apache Airflow to process weather data. The pipeline automates the following tasks:

Extract: Download weather data from Kaggle using the Kaggle API.

Transform: Clean and preprocess the data:

Handle missing values

Remove duplicates

Aggregate daily and monthly statistics

Classify wind strength and determine monthly precipitation type

Validate: Ensure critical columns have no missing values and check for outliers and unexpected ranges.

Load: Store transformed data into a SQLite database, with separate tables for daily and monthly data.

DAG Details

DAG Name: Teamwork_etl_process

Schedule: Daily (@daily)

Owner: Le Thinh

Tasks:

extract_task – Downloads the CSV dataset.

transform_task – Cleans, aggregates, and categorizes the data.

validate_task – Performs data validation and outlier checks.

load_task – Loads the final dataset into the SQLite database.

Setup

Install required packages:

pip install apache-airflow pandas numpy scipy kaggle


Configure your Kaggle API credentials (kaggle.json) in ~/.kaggle/.

Update file paths in the DAG if necessary:

datasets_path = '/home/lethinh/airflow/datasets/'
database_path = '/home/lethinh/airflow/databases/weather_data_2.db'


Start Airflow scheduler and webserver:

airflow db init
airflow scheduler
airflow webserver


Trigger the DAG manually or let it run according to the schedule.

Database Structure

Daily Weather Table

formatted_date, avg_temperature_c, avg_apparent_temperature_c, avg_humidity, avg_wind_speed_kmh, avg_visibility_km, avg_pressure_millibars, wind_strength

Monthly Weather Table

month, avg_temperature_c, avg_apparent_temperature_c, avg_humidity, avg_visibility_km, avg_pressure_millibars, mode_precip_type

Features

Handles missing and duplicate data

Aggregates data daily and monthly

Classifies wind strength into categories

Computes mode of precipitation type per month

Validates data ranges to catch anomalies

Fully automated ETL workflow with Airflow
