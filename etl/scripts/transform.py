import pandas as pd
import json
import os

def transform():
    print("Starting transformation...")

    input_path = "/tmp/raw_data.json"
    output_path = "/tmp/cleaned_data.csv"

    # ---------------------------
    # LOAD RAW DATA
    # ---------------------------

    with open(input_path, "r") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # ---------------------------
    # CLEAN COLUMN NAMES
    # ---------------------------
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(".", "_", regex=False)
        .str.strip()
    )

    # ---------------------------
    # SELECT IMPORTANT COLUMNS
    # ---------------------------
    df = df[[
        "id",
        "name",
        "username",
        "email",
        "address_city",
        "company_name"
    ]]

    # ---------------------------
    # DATA TYPE ENFORCEMENT
    # ---------------------------
    df["id"] = df["id"].astype(int)
    df["name"] = df["name"].astype(str)
    df["email"] = df["email"].astype(str)

    # ---------------------------
    # TEXT STANDARDIZATION
    # ---------------------------
    df["name"] = df["name"].str.title()
    df["username"] = df["username"].str.lower()
    df["email"] = df["email"].str.lower()
    df["address_city"] = df["address_city"].str.title()
    df["company_name"] = df["company_name"].str.title()

    # ---------------------------
    # HANDLE MISSING VALUES
    # ---------------------------
    df.fillna({
        "address_city": "Unknown",
        "company_name": "Unknown"
    }, inplace=True)

    # ---------------------------
    # REMOVE DUPLICATES
    # ---------------------------
    df.drop_duplicates(subset=["id"], inplace=True)

    # ---------------------------
    # FEATURE ENGINEERING
    # ---------------------------
    df["email_domain"] = df["email"].str.split("@").str[-1]
    df["name_length"] = df["name"].str.len()
    df["username_length"] = df["username"].str.len()
    df["location_company"] = df["address_city"] + " - " + df["company_name"]

    # ---------------------------
    # DATA QUALITY CHECKS
    # ---------------------------
    df = df[df["email"].str.contains("@", na=False)]
    df = df[df["name"].str.strip() != ""]

    # ---------------------------
    # SORT & RESET INDEX
    # ---------------------------
    df.sort_values(by="id", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ---------------------------
    # SAVE CLEAN DATA
    # ---------------------------
    df.to_csv(output_path, index=False)

    print(f"Columns in output: {list(df.columns)}")
    print(f"Shape: {df.shape}")
    print(f"Transformation complete. {len(df)} clean records saved to {output_path}")
    return output_path

