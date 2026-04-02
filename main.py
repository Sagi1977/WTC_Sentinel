import os
import datetime
import pandas as pd
import yfinance as yf
import requests
from google import genai  # הספרייה החדשה של 2026
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

# --- מנגנון AI חסין 404 (גילוי דינמי) ---
def get_ai_response(prompt):
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        
        # שלב הגילוי: בודקים מה השם המדויק שגוגל נתנה למודלים שלך היום
        models_list = client.models.list()
        # מחפשים מודל שיש לו 'flash' בשם (הוא הכי מהיר וחינמי)
        target_model = None
        for m in models_list:
            if 'flash' in m.name:
                target_model = m.name
                break
        
        # אם לא מצאנו flash, ניקח את הראשון ברשימה
        if not target_model:
            target_model = 'gemini-1.5-flash' # fallback

        print(f"DEBUG: Using model name: {target_model}")
        
        response = client.models.generate_content(
            model=target_model,
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"שגיאת AI (ניסיון אחרון): {str(e)}"

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

# --- דו"חות ---
def get_institutional_context():
    context_data = ""
    for t in ["^GSPC", "^IXIC", "VIX"]:
        try:
            ticker = yf.Ticker(t)
            news = ticker.news
            if news:
                for n in news[:2]:
                    title = n.get('title') or n.get('content', {}).get('title')
                    if title: context_data += f"- {title}\n"
        except: continue
    
    prompt = f"נתח את הסנטימנט בוול סטריט בעברית: {context_data if context_data else 'אין חדשות חריגות'}"
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
                ticker, score = row['Ticker'], row.get('Score', 0)
                try:
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue
                    if data['Close'].iloc[-1] > data.iloc[:6]['High'].max():
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue
        return f"🥇 Gold: {', '.join(results['Gold']) or 'None'}\n🐕 Underdogs: {', '.join(results['Underdogs']) or 'None'}"
    except Exception as e: return f"שגיאת סריקה: {e}"

# --- המוח המרכזי ---
def main():
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    
    if is_manual:
        send_telegram_msg("🛡️ *WTC Sentinel 2026 - Final Stability Mode*")
        send_telegram_msg(f"🏛️ *Institutional Context:*\n{get_institutional_context()}")
        send_telegram_msg(f"🎯 *Market Scan:*\n{run_execution_scan()}")
        return

    if now.hour == 16:
        send_telegram_msg(f"🏛️ *WTC Context (16:00)*\n\n{get_institutional_context()}")
    elif now.hour == 17:
        send_telegram_msg(f"🎯 *WTC Execution (17:00)*\n\n{run_execution_scan()}")
    elif now.hour == 23:
        send_telegram_msg(f"🌙 *WTC Closing Report*\n\n{get_ai_response('סכם את יום המסחר בעברית')}")

if __name__ == "__main__":
    main()
