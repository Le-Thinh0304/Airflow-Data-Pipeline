# Weather ETL Pipeline with Airflow

## Overview
This repository contains an **ETL (Extract, Transform, Load) pipeline** implemented with **Apache Airflow** to process weather data. The pipeline automates the following tasks:

1. **Extract**: Download weather data from Kaggle using the Kaggle API.  
2. **Transform**: Clean and preprocess the data:
   - Handle missing values  
   - Remove duplicates  
   - Aggregate daily and monthly statistics  
   - Classify wind strength and determine monthly precipitation type  
3. **Validate**: Ensure critical columns have no missing values and check for outliers and unexpected ranges.  
4. **Load**: Store transformed data into a **SQLite database**, with separate tables for daily and monthly data.

---

## DAG Details
- **DAG Name**: `Teamwork_etl_process`  
- **Schedule**: Daily (`@daily`)  
- **Owner**: Le Thinh  
- **Tasks**:
  1. `extract_task` – Downloads the CSV dataset.  
  2. `transform_task` – Cleans, aggregates, and categorizes the data.  
  3. `validate_task` – Performs data validation and outlier checks.  
  4. `load_task` – Loads the final dataset into the SQLite database.  

---

## Setup
1. Install required packages:
```bash
pip install apache-airflow pandas numpy scipy kaggle

