import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import random
from datetime import datetime, timedelta

# Google API permissions
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Load service account credentials from file
creds = Credentials.from_service_account_file(
    "credentials.json",
    scopes=scope
)

# Connect to Google Sheets
client = gspread.authorize(creds)

# Open your sheet
sheet = client.open("data_pipeline_test").sheet1

# Create 5 random rows
sources = ["Meta Ads", "MailerLite", "Asana", "Google Analytics", "Test"]

data_list = []
for i in range(5):
    row = [
        (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d"),
        random.choice(sources),
        random.randint(50, 500)
    ]
    data_list.append(row)

data = pd.DataFrame(data_list, columns=["Date", "Source", "Value"])

# Write to sheet
sheet.clear()
sheet.update([data.columns.values.tolist()] + data.values.tolist())

print("✅ Data written successfully!")