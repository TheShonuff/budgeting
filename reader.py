import csv
import yaml
import pandas as pd
import argparse
import datetime
import gspread
import os.path
from gspread_pandas import Spread, Client

# NOTE: Setup Google Sheets
gc = gspread.service_account()
key = "1Qk1WQ2Jg7utH9aX8_GIt4ZOTeT_3cuO9xSHmns1cODg"
spread = Spread(key)

# Setup Argparse
parser = argparse.ArgumentParser()
# Setup argparse arguments
parser.add_argument("file")
args = parser.parse_args()
print(args.file)

# Setup Export Path
Export_Path = "~/Documents/financial/2023"

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
with open("config.yaml", "r") as config:
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
        Statement_DataFrame.at[index, "Classification"] = "Pets"
    elif any(item in row["Description"].lower() for item in config["buckets"]["tools"]):
        Statement_DataFrame.at[index, "Classification"] = "Tools"
    elif any(item in row["Description"].lower() for item in config["buckets"]["communication"]):
        Statement_DataFrame.at[index, "Classification"] = "Communication"
    elif any(item in row["Description"].lower() for item in config["buckets"]["debt_payment"]):
        Statement_DataFrame.at[index, "Classification"] = "Debt Payment"
    elif any(item in row["Description"].lower() for item in config["buckets"]["transportation"]):
        Statement_DataFrame.at[index, "Classification"] = "Transportation"
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

# Create Summary of a classification groups and sum the totals
month_series_summary = Statement_DataFrame.groupby(["Classification"])["Amount"].sum()
# Convert Series to DataFrame
Month_Summary = pd.Series(month_series_summary).to_frame().reset_index()
# Extract Month and Year from Date column
date_month = pd.to_datetime(Statement_DataFrame.iloc[0]["Date"]).month
date_year = pd.to_datetime(Statement_DataFrame.iloc[0]["Date"]).year

total_deposits = Month_Summary.iloc[4]["Amount"]
# Drop Deposit and Savings rows
Month_Summary = Month_Summary.drop([4, 12, 16]).reset_index(drop=True)
# Calculate Total Expenses
total_expenses = Month_Summary["Amount"].sum()
# Begin preparing Monthly Summary For export
Month_Summary = Month_Summary.assign(Budgeted=0, Result=0, Month=date_month, Year=date_year)
# Month_Summary["Budgeted"] = 0
# Month_Summary["Result"] = 0
# print(total_expenses)
# print(Month_Summary)


Condensed_Monthly_Summary = pd.DataFrame(
    {
        "Date": f"{date_month}/{date_year}",
        "Desposits": total_deposits,
        "Expenses": total_expenses,
        "Outcome": total_deposits + total_expenses,
    },
    index=[0],
)
print(Condensed_Monthly_Summary)

# Insert Budgeted values from config yaml file
for index, row in Month_Summary.iterrows():
    val = Month_Summary["Classification"].loc[index]
    Month_Summary.loc[index, "Budgeted"] = config["budgets"][val]
    Month_Summary.loc[index, "Result"] = Month_Summary.loc[index, "Budgeted"] + Month_Summary.loc[index, "Amount"]

Month_Summary.loc[len(Month_Summary.index)] = ["Total", total_expenses, 0, 0, date_month, date_year]
print(Month_Summary)

# NOTE: Import Monthly Summary to Google Sheets
spread.df_to_sheet(Month_Summary, index=False, sheet="Summary", start="A1", replace=True)

# TODO: Append monthly summary to yearly summaries csv
# TODO export monthly condensed
# Modify arg.file input to be used as an updated export file name
export_summary_str = args.file.replace(".csv", "-summary.csv")
export_str = args.file.replace(".csv", "-classified.csv")
# TODO: Append Monthly Transactions to year transactions csv

# Export CSV as updated monthly standalone file
Statement_DataFrame.to_csv(Export_Path + export_str, index=False)
Month_Summary.to_csv(Export_Path + export_summary_str, index=False)
Month_Summary.to_csv(export_summary_str, index=False)
Statement_DataFrame.to_csv(export_str, index=False)
