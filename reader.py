import csv
import yaml
import pandas as pd
import argparse
import datetime
import gspread
from gspread_pandas import Spread, Client

# Setup Google Sheets
gc = gspread.service_account()
key = "1Qk1WQ2Jg7utH9aX8_GIt4ZOTeT_3cuO9xSHmns1cODg"
# sheet = gc.open_by_key(key)
spread = Spread(key)

# Setup Argparse
parser = argparse.ArgumentParser()
# Setup argparse arguments
parser.add_argument("file")
args = parser.parse_args()
print(args.file)

# Capture Body of CSV
Statement_DataFrame = pd.read_csv(args.file, parse_dates=["Date"], skiprows=5, thousands=",")

# Capture Header Summary
Statement_header_DataFrame = pd.read_csv("feb2023-statement.csv", nrows=4)

# Add column to Statement_DataFrame
Statement_DataFrame["Classification"] = ""

# Delete first row of Statement_DataFrame - no need to categorize "beginning balance"
Statement_DataFrame = Statement_DataFrame.drop(Statement_DataFrame.index[0])

# config['buckets']s: Education, Nicotine, Debt Payments, Medical, Food, Subscription, Transportation, Daycare
# Amazon, Target, Wal-mart, Pets, Tools

# Old method of importing YAML file
# with open("buckets.yaml", "r") as bucket:
#    buckets = yaml.safe_load(bucket)
# Updated YAML import to include additional fields in the config file
with open("dict-buckets.yaml", "r") as config:
    config = yaml.safe_load(config)
# Iterate over DataFrame and Change Classification Column
for index, row in Statement_DataFrame.iterrows():
    # TODO: Refactor this tree
    if any(item in row["Description"].lower() for item in config["buckets"]["medical"]):
        Statement_DataFrame.at[index, "Classification"] = "Medical"
    elif row["Description"].startswith("KEEP THE"):
        Statement_DataFrame.at[index, "Classification"] = "Savings"
    elif any(item in row["Description"].lower() for item in config["buckets"]["savings"]):
        Statement_DataFrame.at[index, "Classification"] = "Savings"
    elif any(item in row["Description"].lower() for item in config["buckets"]["walmart"]):
        Statement_DataFrame.at[index, "Classification"] = "Walmart"
    elif any(item in row["Description"].lower() for item in config["buckets"]["target"]):
        Statement_DataFrame.at[index, "Classification"] = "Target"
    elif any(item in row["Description"].lower() for item in config["buckets"]["transfer"]):
        Statement_DataFrame.at[index, "Classification"] = "Transfer"
    elif any(item in row["Description"].lower() for item in config["buckets"]["food"]):
        Statement_DataFrame.at[index, "Classification"] = "Food"
    elif any(item in row["Description"].lower() for item in config["buckets"]["amazon"]):
        Statement_DataFrame.at[index, "Classification"] = "Amazon"
    elif any(item in row["Description"].lower() for item in config["buckets"]["deposit"]):
        Statement_DataFrame.at[index, "Classification"] = "Deposit"
    elif any(item in row["Description"].lower() for item in config["buckets"]["pets"]):  # This is the way
        Statement_DataFrame.at[index, "Classification"] = "pets"
    elif any(item in row["Description"].lower() for item in config["buckets"]["tools"]):
        Statement_DataFrame.at[index, "Classification"] = "Tools"
    elif any(item in row["Description"].lower() for item in config["buckets"]["communication"]):
        Statement_DataFrame.at[index, "Classification"] = "Communication"
    elif any(item in row["Description"].lower() for item in config["buckets"]["debt_payment"]):
        Statement_DataFrame.at[index, "Classification"] = "Debt Payment"
    elif any(item in row["Description"].lower() for item in config["buckets"]["transportation"]):
        Statement_DataFrame.at[index, "Classification"] = "Transporation"
    elif any(item in row["Description"].lower() for item in config["buckets"]["insurance"]):
        Statement_DataFrame.at[index, "Classification"] = "Insurance"
    elif any(item in row["Description"].lower() for item in config["buckets"]["daycare"]):
        Statement_DataFrame.at[index, "Classification"] = "Daycare"
    elif any(item in row["Description"].lower() for item in config["buckets"]["education"]):
        Statement_DataFrame.at[index, "Classification"] = "Education"
    elif any(item in row["Description"].lower() for item in config["buckets"]["subscription"]):
        Statement_DataFrame.at[index, "Classification"] = "Subscription"
    elif any(item in row["Description"].lower() for item in config["buckets"]["grooming"]):
        Statement_DataFrame.at[index, "Classification"] = "Grooming"
    elif any(item in row["Description"].lower() for item in config["buckets"]["rent"]):
        Statement_DataFrame.at[index, "Classification"] = "Rent"
    else:
        Statement_DataFrame.at[index, "Classification"] = "Uncategorized"
# TODO: Import actual 'Budgeted' values in summary

# Create Summary of a classification groups and sum the totals
month_series_summary = Statement_DataFrame.groupby(["Classification"])["Amount"].sum()
# Convert Series to DataFrame
month_summary = pd.DataFrame(month_series_summary)
# Extract Month and Year from Date column
date_month = pd.to_datetime(Statement_DataFrame.iloc[0]["Date"]).month
date_year = pd.to_datetime(Statement_DataFrame.iloc[0]["Date"]).year
# Create Month and Year columns in Summary DataFrame
month_summary["Month"] = date_month
month_summary["Year"] = date_year

# NOTE: Import Monthly Summary to Google Sheets
# spread.df_to_sheet(month_summary, index=False, sheet="Summary", start="A1", replace=True)

# TODO: Append monthly summary to yearly summaries csv

# Modify arg.file input to be used as an updated export file name
export_str = args.file.replace(".csv", "-updated.csv")
# TODO: Append Monthly Transactions to year transactions csv

# Export CSV as updated monthly standalone file
Statement_DataFrame.to_csv(export_str, index=False)
