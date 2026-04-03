import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from airflow.models import Variable

input_path = "/tmp/cleaned_data.csv"

# ---------------------------
# SNOWFLAKE CREDENTIALS
# ---------------------------
def get_snowflake_config():
    return {
        "account":   Variable.get("SNOWFLAKE_ACCOUNT"),
        "user":      Variable.get("SNOWFLAKE_USER"),
        "password":  Variable.get("SNOWFLAKE_PASSWORD"),
        "database":  Variable.get("SNOWFLAKE_DATABASE"),
        "schema":    Variable.get("SNOWFLAKE_SCHEMA"),
        "warehouse": Variable.get("SNOWFLAKE_WAREHOUSE"),
    }

def get_connection():
    return snowflake.connector.connect(**get_snowflake_config())

def load():
    print("Starting load to Snowflake...")

    # ---------------------------
    # READ CLEANED DATA
    # ---------------------------
    df = pd.read_csv(input_path)
    print(f"Records to load: {len(df)}")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # ---------------------------
        # DIM_USER
        # ---------------------------
        dim_user = df[["id", "name", "username", "email"]].copy()
        dim_user.columns = ["USER_ID", "FULL_NAME", "USERNAME", "EMAIL"]
        dim_user.drop_duplicates(subset=["USER_ID"], inplace=True)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_USER (
                USER_ID     INT PRIMARY KEY,
                FULL_NAME   VARCHAR,
                USERNAME    VARCHAR,
                EMAIL       VARCHAR
            )
        """)
        write_pandas(conn, dim_user, "DIM_USER")
        print(f"DIM_USER loaded: {len(dim_user)} rows")

        # ---------------------------
        # DIM_LOCATION
        # ---------------------------
        dim_location = df[["address_city"]].copy()
        dim_location.drop_duplicates(inplace=True)
        dim_location.reset_index(drop=True, inplace=True)
        dim_location.insert(0, "LOCATION_ID", dim_location.index + 1)
        dim_location.columns = ["LOCATION_ID", "CITY"]

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_LOCATION (
                LOCATION_ID INT PRIMARY KEY,
                CITY        VARCHAR
            )
        """)
        write_pandas(conn, dim_location, "DIM_LOCATION")
        print(f"DIM_LOCATION loaded: {len(dim_location)} rows")

        # ---------------------------
        # DIM_COMPANY
        # ---------------------------
        dim_company = df[["company_name", "email_domain"]].copy()
        dim_company.drop_duplicates(subset=["company_name"], inplace=True)
        dim_company.reset_index(drop=True, inplace=True)
        dim_company.insert(0, "COMPANY_ID", dim_company.index + 1)
        dim_company.columns = ["COMPANY_ID", "COMPANY_NAME", "EMAIL_DOMAIN"]

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_COMPANY (
                COMPANY_ID   INT PRIMARY KEY,
                COMPANY_NAME VARCHAR,
                EMAIL_DOMAIN VARCHAR
            )
        """)
        write_pandas(conn, dim_company, "DIM_COMPANY")
        print(f"DIM_COMPANY loaded: {len(dim_company)} rows")

        # ---------------------------
        # DIM_DATE
        # ---------------------------
        from datetime import date
        today = date.today()
        dim_date = pd.DataFrame([{
            "DATE_ID":    int(today.strftime("%Y%m%d")),
            "LOAD_DATE":  str(today),
            "YEAR":       today.year,
            "MONTH":      today.month,
            "DAY":        today.day
        }])

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DIM_DATE (
                DATE_ID    INT PRIMARY KEY,
                LOAD_DATE  DATE,
                YEAR       INT,
                MONTH      INT,
                DAY        INT
            )
        """)
        write_pandas(conn, dim_date, "DIM_DATE")
        print(f"DIM_DATE loaded: {len(dim_date)} rows")

        # ---------------------------
        # FACT TABLE
        # ---------------------------
        fact = df[["id"]].copy()
        fact.columns = ["USER_ID"]

        # Join foreign keys from each dimension
        fact = fact.merge(
            dim_location.rename(columns={"CITY": "address_city"}),
            left_on=fact["USER_ID"].map(
                df.set_index("id")["address_city"]
            ),
            right_on="address_city",
            how="left"
        )[["USER_ID", "LOCATION_ID"]]

        fact = fact.merge(
            dim_company[["COMPANY_ID", "COMPANY_NAME"]].rename(
                columns={"COMPANY_NAME": "company_name"}
            ),
            left_on=fact["USER_ID"].map(
                df.set_index("id")["company_name"]
            ),
            right_on="company_name",
            how="left"
        )[["USER_ID", "LOCATION_ID", "COMPANY_ID"]]

        today_id = int(date.today().strftime("%Y%m%d"))
        fact["DATE_ID"] = today_id
        fact.columns = ["USER_ID", "LOCATION_ID", "COMPANY_ID", "DATE_ID"]

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS FACT_USER_ACTIVITY (
                USER_ID     INT,
                LOCATION_ID INT,
                COMPANY_ID  INT,
                DATE_ID     INT
            )
        """)
        write_pandas(conn, fact, "FACT_USER_ACTIVITY")
        print(f"FACT_USER_ACTIVITY loaded: {len(fact)} rows")

        print("Load to Snowflake complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    finally:
        cursor.close()
        conn.close()
