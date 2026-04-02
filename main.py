import os
import requests
import google.auth
from googleapiclient.discovery import build

# --- הגדרות טלגרם ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def main():
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    
    try:
        # שליפת פרטי הזהות מהמערכת של גוגל
        creds, project = google.auth.default()
        auth_email = getattr(creds, 'service_account_email', "לא נמצא מייל אוטומטי")
        
        message = (
            "🔑 *זהות הבוט שלך:*\n\n"
            f"`{auth_email}`\n\n"
            "👈 תעתיק את המייל הזה בדיוק כפי שהוא מופיע כאן.\n"
            "לך לתיקייה *WTC_SYSTEM* בדרייב -> שיתוף -> ותוסיף אותו כ-Editor."
        )
        
        requests.post(url, json={"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"})
        print(f"Identity sent to Telegram: {auth_email}")

    except Exception as e:
        error_msg = f"❌ שגיאה בשליפת הזהות: {str(e)}"
        requests.post(url, json={"chat_id": CHAT_ID, "text": error_msg})

if __name__ == "__main__":
    main()
