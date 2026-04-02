# Retail ETL Pipeline — Apache Airflow & Snowflake

An automated end-to-end data pipeline that extracts retail user data from a REST API, applies structured transformations and feature engineering, and loads a Star Schema data warehouse into Snowflake — orchestrated and scheduled with Apache Airflow.

---

## Architecture Overview

```
REST API (JSONPlaceholder)
        │
        ▼
  [ Extract Task ]
  Fetches raw JSON data
  Saves to /tmp/raw_data.json
        │
        ▼
  [ Transform Task ]
  Cleans, standardises & engineers features
  Saves to /tmp/cleaned_data.csv
        │
        ▼
  [ Load Task ]
  Builds Star Schema
  Loads 4 tables into Snowflake
        │
        ▼
  Snowflake Data Warehouse (RETAIL_DW.STAR)
  ├── DIM_USER
  ├── DIM_LOCATION
  ├── DIM_COMPANY
  ├── DIM_DATE
  └── FACT_USER_ACTIVITY
```

The pipeline runs daily at 06:00 UTC, with automatic retries on failure and email alerting via Gmail SMTP.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.x |
| Language | Python 3.12 |
| Data Processing | Pandas |
| Data Warehouse | Snowflake (Star Schema) |
| Connector | snowflake-connector-python |
| Environment | WSL 2 (Ubuntu 24) on Windows |
| Source | JSONPlaceholder REST API |
| Alerting | Gmail SMTP |
| Version Control | Git & GitHub |

---

## Star Schema Design

```
                  ┌─────────────┐
                  │  DIM_USER   │
                  │─────────────│
                  │ user_id  PK │
                  │ full_name   │
                  │ username    │
                  │ email       │
                  └──────┬──────┘
                         │
       ┌─────────────────┼─────────────────┐
       │                 │                 │
┌──────┴──────┐  ┌───────┴───────┐  ┌──────┴──────┐
│DIM_LOCATION │  │FACT_USER_     │  │ DIM_COMPANY │
│─────────────│  │ACTIVITY       │  │─────────────│
│location_id  │  │───────────────│  │ company_id  │
│city         │◄─│user_id     FK │  │ company_name│
└─────────────┘  │location_id FK │─►└─────────────┘
                 │company_id  FK │
                 │date_id     FK │
                 └───────┬───────┘
                         │
                  ┌──────┴──────┐
                  │  DIM_DATE   │
                  │─────────────│
                  │ date_id  PK │
                  │ load_date   │
                  │ year        │
                  │ month       │
                  │ day         │
                  └─────────────┘
```

---

## Pipeline Walkthrough

### Extract
- Connects to the JSONPlaceholder REST API
- Fetches 10 user records in JSON format
- Saves raw data to `/tmp/raw_data.json`
- Returns the output path via Airflow XCom

### Transform
- Loads raw JSON using `pd.json_normalize()` for automatic flattening of nested fields
- Cleans and standardises column names to snake_case
- Selects relevant columns: `id`, `name`, `username`, `email`, `address_city`, `company_name`
- Enforces data types and text standardisation (title case, lowercase)
- Handles missing values with sensible defaults
- Removes duplicate records
- Applies feature engineering:
  - `email_domain` — extracted from email address
  - `name_length` — character count of full name
  - `username_length` — character count of username
  - `location_company` — combined city and company field
- Runs data quality checks (valid email format, non-empty names)
- Saves cleaned data to `/tmp/cleaned_data.csv`

### Load
- Reads cleaned CSV into a Pandas DataFrame
- Connects to Snowflake using credentials stored in Airflow Variables
- Creates and populates 3 dimension tables and 1 fact table
- Uses `write_pandas()` for efficient bulk loading
- Implements `try/except/finally` for safe connection handling

---

## DAG Configuration

```
>python
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    dag_id="retail_etl_pipeline",
    schedule="0 6 * * *",        # Daily at 06:00 UTC
    catchup=False,
    is_paused_upon_creation=False,
) as dag:
    extract >> transform >> load
```

---

## Project Structure

```
├── airflow/
│   └── dags/
│       └── retail_dag.py          # Airflow DAG definition
└── etl/
    └── scripts/
        ├── extract.py             # API extraction logic
        ├── transform.py           # Data cleaning & feature engineering
        └── load.py                # Snowflake Star Schema loader
```

---

## Setup Instructions

### Prerequisites
- Windows 10/11 with WSL 2 enabled
- Python 3.12
- A Snowflake account (free trial available at snowflake.com)
- A Gmail account with App Password enabled

### 1. Clone the repository
```
> bash
git clone https://github.com/yourusername/retail-etl-pipeline.git
cd retail-etl-pipeline
```

### 2. Create and activate a virtual environment
```
> bash
python3 -m venv airflow-venv
source airflow-venv/bin/activate
```

### 3. Install dependencies
```
> bash
pip install apache-airflow==2.9.1
pip install pandas requests "snowflake-connector-python[pandas]"
```

### 4. Initialise Airflow
```
> bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
  --username admin --role Admin \
  --firstname Admin --lastname User \
  --email admin@example.com --password admin123
```

### 5. Set Airflow Variables
```
> bash
airflow variables set SNOWFLAKE_ACCOUNT "your_account"
airflow variables set SNOWFLAKE_USER "your_username"
airflow variables set SNOWFLAKE_PASSWORD "your_password"
airflow variables set SNOWFLAKE_DATABASE "RETAIL_DW"
airflow variables set SNOWFLAKE_SCHEMA "STAR"
airflow variables set SNOWFLAKE_WAREHOUSE "COMPUTE_WH"
airflow variables set ALERT_EMAIL "your_email@gmail.com"
```

### 6. Configure Gmail SMTP in airflow.cfg
```
> ini
[smtp]
smtp_host = smtp.gmail.com
smtp_port = 465
smtp_ssl = True
smtp_starttls = False
smtp_user = your_email@gmail.com
smtp_password = your_app_password
smtp_mail_from = your_email@gmail.com
```

### 7. Create Snowflake database
Run the following in a Snowflake worksheet:
```
> sql
CREATE DATABASE IF NOT EXISTS RETAIL_DW;
CREATE SCHEMA IF NOT EXISTS RETAIL_DW.STAR;
```

### 8. Start Airflow and run the pipeline
```
> bash
airflow standalone
```
Then visit `http://localhost:8080`, log in and trigger `retail_etl_pipeline`.

---

## Screenshots

> Add the following screenshots to a `/screenshots` folder in your repo:
> - `airflow_dag_success.png` — Graph view showing all 3 tasks green
> - `snowflake_tables.png` — Database explorer showing all 5 tables
> - `snowflake_query.png` — Star schema JOIN query results
> - `email_alert.png` — Gmail failure alert email

---

## Key Concepts Demonstrated

- **ETL Pipeline Design** — separation of extract, transform and load concerns into independent, testable modules
- **Data Warehouse Modelling** — Star Schema design with fact and dimension tables
- **Pipeline Orchestration** — DAG definition, task dependencies, scheduling and monitoring with Apache Airflow
- **Data Quality** — validation checks, duplicate removal, missing value handling and type enforcement
- **Feature Engineering** — derived columns adding analytical value to raw data
- **Production Hardening** — retries, timeouts, email alerting and secrets management via Airflow Variables
- **Cloud Integration** — loading data to Snowflake using the official Python connector

---

## Author

**Benjamin Ejimbe**
Data Engineering Portfolio Project
[GitHub](https://github.com/Benness001) · [LinkedIn](https://linkedin.com/in/benjamin-ejimbe-7a32bb387)
