import gspread
from gspread_pandas import Spread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from datetime import datetime

gc = gspread.service_account()
sh = gc.open("Application-Test")


# TODO: Add argparse to allow file names to be custom loaded
def convert_date(date):
    date = str(date)
    date = pd.Timestamp(date)
    date = date.strftime("%m-%d-%Y")
    return date


worksheet_title = "2023"
try:
    worksheet = sh.worksheet(worksheet_title)
except gspread.WorksheetNotFound:
    worksheet = sh.add_worksheet(title=worksheet_title, rows=77, cols=16)


Master_Record = get_as_dataframe(worksheet, na_value=" NaN", parse_dates=["Entry Date"])

## Read in downloaded Data
new_applicants = pd.read_csv("foh-sample.csv", parse_dates=["Entry Date"])
# Sanitze DataFrame
new_applicants = new_applicants.drop(new_applicants.iloc[:, 17:38], axis=1)
new_applicants = new_applicants.drop(new_applicants.columns[[0, 1, 2, 4, 5, 9, 14, 17, 19, 20, 22, 23]], axis=1)

# NOTE: The Concat method works best for the desired results
Master_Record = pd.concat([Master_Record, new_applicants], ignore_index=True).reset_index(drop=True)
for index, row in Master_Record.iterrows():
    date = Master_Record.loc[index, "Entry Date"]
    Master_Record.loc[index, "Entry Date"] = convert_date(date)

Master_Record = Master_Record.sort_values(by=["Entry Date"], ascending=False)
print(Master_Record["Entry Date"].dtype)
Master_Record.to_csv("test1.csv")

Master_Record = set_with_dataframe(worksheet, Master_Record, include_column_header=True)
