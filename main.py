import os
import datetime
import time
import pandas as pd
import yfinance as yf
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.auth
import io

# --- הגדרות ליבה ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "Markdown"})
    time.sleep(1)

def get_identity_and_service():
    creds, project = google.auth.default(scopes=['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/drive.metadata.readonly'])
    service = build('drive', 'v3', credentials=creds)
    
    # ניסיון למצוא את המייל של הבוט
    try:
        user_info = service.about().get(fields="user(emailAddress)").execute()
        email = user_info['user']['emailAddress']
    except:
        # אם ה-API של 'About' חסום, ננסה דרך ה-Credentials
        email = getattr(creds, 'service_account_email', "Unknown (Check Google Cloud Console)")
    
    return email, service

def run_diagnostic_scan(service):
    # בדיקת ראייה: אילו תיקיות הבוט רואה עכשיו?
    results = service.files().list(q="mimeType = 'application/vnd.google-apps.folder'", fields="files(name)").execute()
    folders = [f['name'] for f in results.get('files', [])]
    
    if "WTC_SYSTEM" in [f.upper() for f in folders]:
        return "✅ הבוט רואה את התיקייה WTC_SYSTEM! הסריקה תעבוד."
    else:
        return f"❌ הבוט לא רואה את התיקייה. תיקיות גלויות: {', '.join(folders) if folders else 'אין תיקיות משותפות'}"

def main():
    try:
        bot_email, service = get_identity_and_service()
        
        # הודעה 1: זהות הבוט (הכי חשוב!)
        identity_msg = (
            f"🔑 *Bot Identity (Share with this Email):*\n"
            f"`{bot_email}`\n\n"
            f"👈 תעתיק את המייל הזה ותוסיף אותו כ-Editor לתיקייה WTC_SYSTEM בדרייב."
        )
        send_telegram_msg(identity_msg)
        
        # הודעה 2: בדיקת ראייה
        diagnostic_msg = run_diagnostic_scan(service)
        send_telegram_msg(f"📡 *Diagnostic:* {diagnostic_msg}")
        
    except Exception as e:
        send_telegram_msg(f"⚠️ שגיאת מערכת: {str(e)}")

if __name__ == "__main__":
    main()
