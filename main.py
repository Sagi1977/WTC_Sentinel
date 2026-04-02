import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.auth
import io

# --- הגדרות וסודות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

# --- מנגנון AI חכם ---
def get_ai_response(prompt):
    try:
        available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        selected_model = next((m for m in available_models if '1.5-flash' in m), available_models[0])
        model = genai.GenerativeModel(selected_model)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"שגיאת AI: {str(e)}"

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

# --- פונקציות הדו"חות ---
def get_institutional_context():
    context_data = ""
    for t in ["^GSPC", "^IXIC", "VIX"]:
        news = yf.Ticker(t).news[:2]
        for n in news: context_data += f"- {n['title']}\n"
    
    prompt = f"אתה אנליסט מוסדי. נתח את החדשות הבאות בעברית: {context_data}. מה הכסף הגדול חושב היום? האם יש הודעות פד/מאקרו?"
    return get_ai_response(prompt)

def run_execution_scan():
    service = get_drive_service()
    df_stocks = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_Stocks")
    df_etfs = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_ETFs")
    
    results = {"Gold": [], "Underdogs": []}
    
    for df in [df_stocks, df_etfs]:
        if df is None: continue
        for _, row in df.iterrows():
            ticker, score = row['Ticker'], row['Score']
            try:
                data = yf.download(ticker, period="1d", interval="5m", progress=False)
                if len(data) < 7: continue
                if data['Close'].iloc[-1] > data.iloc[:6]['High'].max():
                    if score >= 75: results["Gold"].append(ticker)
                    elif score < 60: results["Underdogs"].append(ticker)
            except: continue
    
    report = f"🥇 *Gold Breakouts:* {', '.join(results['Gold']) if results['Gold'] else 'None'}\n"
    report += f"🐕 *Underdog Breakouts:* {', '.join(results['Underdogs']) if results['Underdogs'] else 'None'}"
    return report

# --- המוח המרכזי ---
def main():
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now_israel = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now_israel.hour

    if is_manual:
        send_telegram_msg("🧪 *WTC Sentinel Health Check*")
        send_telegram_msg(f"🏛️ *Context:*\n{get_institutional_context()}")
        send_telegram_msg(f"🎯 *Current Scan:*\n{run_execution_scan()}")
        return

    if hour == 16:
        send_telegram_msg(f"🏛️ *WTC Institutional Intelligence (16:00)*\n\n{get_institutional_context()}")
    elif hour == 17:
        send_telegram_msg(f"🎯 *WTC Execution Report (17:00)*\n\n{run_execution_scan()}")
    elif hour == 23:
        summary = get_ai_response("סכם את יום המסחר בוול סטריט בעברית ומה התובנה למחר?")
        send_telegram_msg(f"🌙 *WTC Daily Closing Summary*\n\n{summary}")

if __name__ == "__main__":
    main()
