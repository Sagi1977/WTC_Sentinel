import os
import pandas as pd
import yfinance as yf
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.auth
import io

# --- הגדרות טלגרם ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

# --- חיבור לגוגל דרייב ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_csv(service, folder_name, file_prefix):
    # חיפוש התיקייה
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    if not items: return None
    folder_id = items[0]['id']

    # חיפוש הקובץ הכי חדש שמתחיל בקידומת מסוימת
    query = f"'{folder_id}' in parents and name contains '{file_prefix}' and mimeType = 'text/csv'"
    results = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
    files = results.get('files', [])
    
    if not files: return None
    
    file_id = files[0]['id']
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

# --- לוגיקת הסריקה (Execution) ---
def run_sentinel_scan():
    service = get_drive_service()
    
    # טעינת נתונים
    df_stocks = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_Stocks")
    df_etfs = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_ETFs")
    
    results = {"Gold_Stocks": [], "Gold_ETFs": [], "Underdogs_Stocks": [], "Underdogs_ETFs": []}
    
    def process_df(df, is_etf):
        if df is None: return
        category = "ETFs" if is_etf else "Stocks"
        for _, row in df.iterrows():
            ticker = row['Ticker']
            score = row['Score']
            try:
                data = yf.download(ticker, period="1d", interval="5m", progress=False)
                if data.empty or len(data) < 7: continue
                
                opening_high = data.iloc[:6]['High'].max()
                current_price = data['Close'].iloc[-1]
                
                if current_price > opening_high:
                    if score >= 75:
                        results[f"Gold_{category}"].append(ticker)
                    elif score < 60:
                        results[f"Underdogs_{category}"].append(f"{ticker}({int(score)})")
            except: continue

    process_df(df_stocks, False)
    process_df(df_etfs, True)

    # בניית הדו"ח
    report = [
        "🎯 *WTC 17:00 Execution Report*",
        "--------------------------",
        f"🥇 *Gold Stocks:* {', '.join(results['Gold_Stocks']) if results['Gold_Stocks'] else 'None'}",
        f"💎 *Gold ETFs:* {', '.join(results['Gold_ETFs']) if results['Gold_ETFs'] else 'None'}",
        "--------------------------",
        f"🐕 *Underdogs Stocks:* {', '.join(results['Underdogs_Stocks']) if results['Underdogs_Stocks'] else 'None'}",
        f"🐾 *Underdogs ETFs:* {', '.join(results['Underdogs_ETFs']) if results['Underdogs_ETFs'] else 'None'}",
        "--------------------------",
        "💡 *Action:* Watch Gold for entries."
    ]
    
    send_telegram_msg("\n".join(report))

if __name__ == "__main__":
    print("🚀 Sentinel is waking up...")
    run_sentinel_scan()
    print("✅ Done.")
