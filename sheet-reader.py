import gspread
from gspread_pandas import Spread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from datetime import datetime
import argparse

gc = gspread.service_account()
# TODO: point at live file
sh = gc.open("Application-Test")

# Setup ArgParge to allow for custom file names
parser = argparse.ArgumentParser(description="Reads in a CSV file and appends it to a Google Sheet")
parser.add_argument("file")
args = parser.parse_args()


def convert_date(date):
    date = str(date)
    date = pd.Timestamp(date)
    # date = date.strftime("%m-%d-%Y")
    return date


worksheet_title = "2023"
try:
    worksheet = sh.worksheet(worksheet_title)
except gspread.WorksheetNotFound:
    worksheet = sh.add_worksheet(title=worksheet_title, rows=77, cols=16)


Master_Record = get_as_dataframe(worksheet, na_value=" NaN", parse_dates=["Entry Date"], encoding="utf-8")
for index, row in Master_Record.iterrows():
    if pd.isnull(Master_Record["Approved?"][index]):
        Master_Record.loc[index, "Approved?"] = 0.0
    elif pd.isnull(Master_Record["Madison Contact - Denials"][index]):
        Master_Record.loc[index, "Madison Contact - Denials"] = 0.0

Master_Record["Approved?"] = Master_Record["Approved?"].astype(bool)
Master_Record["Madison Contact - Denials"] = Master_Record["Madison Contact - Denials"].astype(bool)
print(Master_Record["Approved?"].head())

## Read in downloaded Data
# new_applicants = pd.read_csv("foh-sample.csv", parse_dates=["Entry Date"], encoding="utf-8")
# Read in file from ArgumentParser
new_applicants = pd.read_csv(args.file, parse_dates=["Entry Date"])
# Sanitze DataFrame
new_applicants = new_applicants.drop(new_applicants.iloc[:, 17:38], axis=1)
new_applicants = new_applicants.drop(new_applicants.columns[[0, 1, 2, 4, 5, 9, 14, 17, 19, 20, 22, 23]], axis=1)

# NOTE: The Concat method works best for the desired results
Master_Record = pd.concat([Master_Record, new_applicants], ignore_index=True).reset_index(drop=True)
for index, row in Master_Record.iterrows():
    date = Master_Record.loc[index, "Entry Date"]
    Master_Record.loc[index, "Entry Date"] = convert_date(date)
Master_Record["Entry Date"] = Master_Record["Entry Date"].dt.strftime("%Y-%m-%d")
# TODO: Preserve Checkboxes
Master_Record = Master_Record.drop_duplicates(subset=["Patient Name", "Entry Date"], keep="first")
Master_Record = Master_Record.sort_values(by=["Entry Date"], ascending=False)

Master_Record = set_with_dataframe(worksheet, Master_Record, include_column_header=True)
