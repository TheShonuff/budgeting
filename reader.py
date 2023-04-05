import csv
from os import path
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
parser.add_argument("-c", "--classify", action="store_true", required=False, help="Add classifications to transactions")
parser.add_argument(
    "-e", "--export", action="store_true", required=False, help="Exports to Google sheets and summaries"
)
args = parser.parse_args()
print(f"Starting to process {args.file}")
# Setup Export Path
Export_Path = "~/Documents/financial/2023/"

# Dates Dictionary for file export names
dates = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sept",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


# Define Export function
def export(date, dataframe, dataframe_summary):
    date_format = dates.get(date)
    export_summary_str = f"{date_format}-report.csv"
    export_str = f"{date_format}-statement.csv"
    dataframe.to_csv(Export_Path + export_str, index=False)
    dataframe_summary.to_csv(Export_Path + export_summary_str, index=False)


# Capture Body of CSV
Statement_DataFrame = pd.read_csv(args.file, parse_dates=["Date"], skiprows=5, thousands=",")

# Capture Header Summary
Statement_header_DataFrame = pd.read_csv("feb2023-statement.csv", nrows=4)

# Add column to Statement_DataFrame
Statement_DataFrame["Classification"] = ""

# Delete first row of Statement_DataFrame - no need to categorize "beginning balance"
Statement_DataFrame = Statement_DataFrame.drop(Statement_DataFrame.index[0])

# TODO: SETUP XDG to safely move script to /bin/

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

Monthly_Deposits_vs_Expenses = pd.DataFrame(
    {
        "Date": f"{date_month}/{date_year}",
        "Desposits": total_deposits,
        "Expenses": total_expenses,
        "Outcome": total_deposits + total_expenses,
    },
    index=[0],
)

# Insert Budgeted values from config yaml file
for index, row in Month_Summary.iterrows():
    val = Month_Summary["Classification"].loc[index]
    Month_Summary.loc[index, "Budgeted"] = config["budgets"][val]
    Month_Summary.loc[index, "Result"] = Month_Summary.loc[index, "Budgeted"] + Month_Summary.loc[index, "Amount"]

Month_Summary.loc[len(Month_Summary.index)] = ["Total", total_expenses, 0, 0, date_month, date_year]

Monthly_Report = Monthly_Deposits_vs_Expenses.join(Month_Summary, how="right")

# TODO: Create Year Summary xlsx file
# Create 2023 Year DataFrame
# TODO: Check if year_summary.csv exists in 2023 folder

# Summary = "/Documents/financial/2023/Year-Summary.csv"
# if os.path.isfile(Summary):

if args.export:
    # NOTE: Import Monthly Summary to Google Sheets
    spread.df_to_sheet(Monthly_Report, index=False, sheet=f"{dates.get(date_month)}-Summary", start="A1", replace=True)
    print(f"successfully updated {args.file} to Google Sheets")
    export(date_month, Statement_DataFrame, Monthly_Report)
    print(f"Success! Exported csv data to {Export_Path}")
else:
    export(date_month, Statement_DataFrame, Monthly_Report)
    print(f"Success! Exported csv data to {Export_Path}")
