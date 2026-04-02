import os
import datetime
import pandas as pd
import yfinance as yf
import requests
from google import genai # הספרייה החדשה
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.auth
import io

# --- הגדרות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

# --- מנגנון AI חדיש (2026 Ready) ---
def get_ai_response(prompt):
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        # משתמשים במודל הכי חזק וזמין כרגע
        response = client.models.generate_content(
            model='gemini-1.5-flash', 
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"שגיאת AI (2026 SDK): {str(e)}"

# --- חיבור לגוגל דרייב ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_csv(service, folder_name, file_prefix):
    try:
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
        results = service.files().list(q=query, fields="files(id)").execute()
        items = results.get('files', [])
        if not items: return None
        folder_id = items[0]['id']

        query = f"'{folder_id}' in parents and name contains '{file_prefix}' and mimeType = 'text/csv'"
        results = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
        files = results.get('files', [])
        if not files: return None
        
        request = service.files().get_media(fileId=files[0]['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    except: return None

# --- דו"חות חסינים ---
def get_institutional_context():
    context_data = ""
    for t in ["^GSPC", "^IXIC", "VIX"]:
        try:
            ticker = yf.Ticker(t)
            news = ticker.news
            if news:
                # הגנה מפני שינויים במבנה של yfinance
                for n in news[:2]:
                    title = n.get('title') or n.get('content', {}).get('title')
                    if title:
                        context_data += f"- {title}\n"
        except: continue
    
    if not context_data:
        context_data = "לא נמצאו חדשות חריגות כרגע, מנתח לפי תנועת מחיר בלבד."
        
    prompt = f"אתה אנליסט מוסדי. נתח את הסנטימנט של וול סטריט בעברית לפי המידע הבא: {context_data}"
    return get_ai_response(prompt)

def run_execution_scan():
    try:
        service = get_drive_service()
        df_stocks = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_Stocks")
        df_etfs = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_ETFs")
        
        results = {"Gold": [], "Underdogs": []}
        
        for df in [df_stocks, df_etfs]:
            if df is None: continue
            for _, row in df.iterrows():
                ticker = row['Ticker']
                score = row.get('Score', 0)
                try:
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue
                    if data['Close'].iloc[-1] > data.iloc[:6]['High'].max():
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue
        
        report = f"🥇 *Gold:* {', '.join(results['Gold']) if results['Gold'] else 'None'}\n"
        report += f"🐕 *Underdogs:* {', '.join(results['Underdogs']) if results['Underdogs'] else 'None'}"
        return report
    except Exception as e:
        return f"שגיאה בסריקה: {str(e)}"

# --- המוח המרכזי ---
def main():
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now_israel = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now_israel.hour

    if is_manual:
        send_telegram_msg("🛡️ *WTC Sentinel 2026 - Manual Mode*")
        # מריצים את ה-Context בנפרד כדי שאם הוא ייכשל, הסריקה עדיין תעבוד
        try:
            ctx = get_institutional_context()
            send_telegram_msg(f"🏛️ *Institutional Context:*\n{ctx}")
        except Exception as e:
            send_telegram_msg(f"⚠️ שגיאה בדווח מוסדי: {e}")
            
        try:
            scan = run_execution_scan()
            send_telegram_msg(f"🎯 *Market Scan:*\n{scan}")
        except Exception as e:
            send_telegram_msg(f"⚠️ שגיאה בסריקת מניות: {e}")
        return

    # הרצה אוטומטית (נשאר לפי השעות שקבענו)
    if hour == 16:
        send_telegram_msg(f"🏛️ *WTC Context (16:00)*\n\n{get_institutional_context()}")
    elif hour == 17:
        send_telegram_msg(f"🎯 *WTC Execution (17:00)*\n\n{run_execution_scan()}")
    elif hour == 23:
        summary = get_ai_response("סכם את יום המסחר בוול סטריט בעברית.")
        send_telegram_msg(f"🌙 *WTC Closing Report*\n\n{summary}")

if __name__ == "__main__":
    main()
