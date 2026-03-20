import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import random
from datetime import datetime, timedelta
import os
import json

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

sheet = client.open("data_pipeline_test").sheet1

sources = ["Meta Ads", "MailerLite", "Asana", "Google Analytics", "Test"]
run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

data_list = []
for i in range(5):
    row = [
        run_time,
        (datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d"),
        random.choice(sources),
        random.randint(50, 500)
    ]
    data_list.append(row)

data = pd.DataFrame(data_list, columns=["Run Timestamp", "Date", "Source", "Value"])

sheet.clear()
sheet.update([data.columns.values.tolist()] + data.values.tolist())

print("✅ Data written successfully!")
