import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import json
import requests
from datetime import datetime
 
# ── Google Sheets Auth ──────────────────────────────────────────────────────
scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)
sheet_doc = client.open("Digital_Data_exp")
 
# ── MailerLite Auth ─────────────────────────────────────────────────────────
MAILERLITE_API_KEY = os.environ["MAILERLITE_API_KEY"]
HEADERS = {
    "Authorization": f"Bearer {MAILERLITE_API_KEY}",
    "Content-Type": "application/json"
}
BASE_URL = "https://connect.mailerlite.com/api"
 
 
# ── Helper: paginate through all results ───────────────────────────────────
def fetch_all(endpoint, params=None):
    results = []
    params = params or {}
    params["limit"] = 100
    params["page"] = 1
    while True:
        response = requests.get(f"{BASE_URL}/{endpoint}", headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()
        results.extend(data["data"])
        meta = data.get("meta", {})
        current_page = meta.get("current_page", 1)
        last_page = meta.get("last_page", 1)
        if current_page >= last_page:
            break
        params["page"] += 1
    return results
 
 
# ── 1. CAMPAIGNS ────────────────────────────────────────────────────────────
print("📥 Fetching campaigns...")
campaigns_raw = fetch_all("campaigns", params={"filter[status]": "sent"})
 
campaigns_data = []
for c in campaigns_raw:
    stats = c.get("stats", {})
    campaigns_data.append({
        "campaign_id":          c.get("id"),
        "campaign_name":        c.get("name"),
        "status":               c.get("status"),
        "type":                 c.get("type"),
        "subject":              c["emails"][0].get("subject") if c.get("emails") else None,
        "from_name":            c["emails"][0].get("from_name") if c.get("emails") else None,
        "scheduled_for":        c.get("scheduled_for"),
        "finished_at":          c.get("finished_at"),
        "sent":                 stats.get("sent"),
        "deliveries_count":     stats.get("deliveries_count"),
        "delivery_rate":        stats.get("delivery_rate", {}).get("string"),
        "opens_count":          stats.get("opens_count"),
        "open_rate":            stats.get("open_rate", {}).get("string"),
        "clicks_count":         stats.get("clicks_count"),
        "click_rate":           stats.get("click_rate", {}).get("string"),
        "click_to_open_rate":   stats.get("click_to_open_rate", {}).get("string"),
        "unsubscribes_count":   stats.get("unsubscribes_count"),
        "unsubscribe_rate":     stats.get("unsubscribe_rate", {}).get("string"),
        "hard_bounces_count":   stats.get("hard_bounces_count"),
        "soft_bounces_count":   stats.get("soft_bounces_count"),
        "spam_count":           stats.get("spam_count"),
        "last_updated":         datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    })
 
campaigns_df = pd.DataFrame(campaigns_data)
 
try:
    campaigns_sheet = sheet_doc.worksheet("MailerLite_Campaigns")
except gspread.exceptions.WorksheetNotFound:
    campaigns_sheet = sheet_doc.add_worksheet(title="MailerLite_Campaigns", rows=1000, cols=30)
 
campaigns_sheet.clear()
campaigns_sheet.update([campaigns_df.columns.tolist()] + campaigns_df.values.tolist())
print(f"✅ Campaigns written: {len(campaigns_df)} rows")
 
 
# ── 2. SUBSCRIBERS ──────────────────────────────────────────────────────────
print("📥 Fetching subscribers...")
subscribers_raw = fetch_all("subscribers")
 
subscribers_data = []
for s in subscribers_raw:
    subscribers_data.append({
        "subscriber_id":    s.get("id"),
        "email":            s.get("email"),
        "status":           s.get("status"),
        "source":           s.get("source"),
        "sent":             s.get("sent"),
        "opens_count":      s.get("opens_count"),
        "clicks_count":     s.get("clicks_count"),
        "open_rate":        s.get("open_rate"),
        "click_rate":       s.get("click_rate"),
        "subscribed_at":    s.get("subscribed_at"),
        "unsubscribed_at":  s.get("unsubscribed_at"),
        "created_at":       s.get("created_at"),
        "updated_at":       s.get("updated_at"),
        "name":             s.get("fields", {}).get("name"),
        "last_name":        s.get("fields", {}).get("last_name"),
        "country":          s.get("fields", {}).get("country"),
        "last_updated":     datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    })
 
subscribers_df = pd.DataFrame(subscribers_data)
 
try:
    subscribers_sheet = sheet_doc.worksheet("MailerLite_Subscribers")
except gspread.exceptions.WorksheetNotFound:
    subscribers_sheet = sheet_doc.add_worksheet(title="MailerLite_Subscribers", rows=5000, cols=20)
 
subscribers_sheet.clear()
subscribers_sheet.update([subscribers_df.columns.tolist()] + subscribers_df.values.tolist())
print(f"✅ Subscribers written: {len(subscribers_df)} rows")
 
print("🎉 MailerLite pipeline complete!")
