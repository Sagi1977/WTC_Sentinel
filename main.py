import os
import time
import io
import pandas as pd
import yfinance as yf
import requests
import datetime
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# הגדרות סביבה
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
BASE = f"https://api.telegram.org/bot{TOKEN}"

def send_msg(text):
    if not text: return
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(f"{BASE}/sendMessage", 
                         json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "Markdown"}, 
                         timeout=10)
        except: pass
        time.sleep(0.5)

def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)

def download_latest_file(service, prefix):
    try:
        res = service.files().list(q=f"name contains '{prefix}'", orderBy="createdTime desc").execute()
        files = res.get("files", [])
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
    except: return None

def get_latest_monday_open(ticker_data):
    """מוצא את מחיר הפתיחה של יום שני האחרון בתוך דאטה-פריים קיים"""
    try:
        if ticker_data.empty: return None
        et_idx = ticker_data.index.tz_convert("America/New_York")
        # מציאת יום שני האחרון שמופיע בנתונים
        mondays = sorted(list(set([ts.date() for ts in et_idx if ts.weekday() == 0])), reverse=True)
        if not mondays: return None
        
        last_monday = mondays[0]
        monday_data = ticker_data[et_idx.date == last_monday]
        
        # מחפש את המחיר הכי קרוב ל-10:00 AM
        for hour in [10, 11, 12]: # Fallback לשעות מאוחרות יותר אם חסר דאטה
            target = monday_data.between_time(f"{hour}:00", f"{hour}:15")
            if not target.empty: return target.iloc[0]['Open']
        return monday_data.iloc[0]['Open']
    except: return None

def process_sentinel():
    service = get_drive_service()
    
    # 1. טעינת קובץ ה-Priority
    df_p = download_latest_file(service, "Smart_Priority_List")
    if df_p is None:
        send_msg("❌ שגיאה: לא נמצא קובץ Priority בדרייב.")
        return

    tickers = df_p['symbol'].unique().tolist()
    
    # 2. הורדה קבוצתית (Batch) - מהיר בהרבה
    print(f"🚀 Downloading data for {len(tickers)} tickers...")
    full_data = yf.download(tickers, period="7d", interval="5m", group_by='ticker', threads=True)
    
    # 3. משיכת VIX למדד פחד
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    
    results = {"STOCKS": [], "ETF": []}
    
    for ticker in tickers:
        try:
            # חילוץ נתונים למניה ספציפית מה-Batch
            t_data = full_data[ticker].dropna()
            if t_data.empty: continue
            
            curr_price = t_data['Close'].iloc[-1]
            prev_close = t_data['Close'].iloc[-2] if len(t_data) > 1 else t_data['Open'].iloc[-1]
            day_chg = ((curr_price / t_data[t_data.index.date == t_data.index[-1].date()]['Open'].iloc[0]) - 1) * 100
            
            # חיבור לבאג יום שני המתוקן
            monday_open = get_latest_monday_open(t_data)
            wk_chg = ((curr_price / monday_open) - 1) * 100 if monday_open else 0
            
            score = df_p[df_p['symbol'] == ticker]['final_score'].values[0]
            is_etf = df_p[df_p['symbol'] == ticker]['is_etf'].values[0]
            
            # פילטר ה-UnderDogs (אלו שמתפוצצים השבוע)
            if wk_chg > 5 and score > 75:
                cat = "ETF" if is_etf else "STOCKS"
                results[cat].append((ticker, curr_price, day_chg, wk_chg, score))
        except: continue

    # 4. בניית הדוח
    report = f"🛰️ *WTC Sentinel Dashboard* | {datetime.datetime.now().strftime('%H:%M')}\n"
    report += f"📊 *Market VIX:* `{vix:.2f}` {'⚠️' if vix > 22 else '✅'}\n"
    report += "`-----------------------------`\n\n"
    
    for section in ["STOCKS", "ETF"]:
        report += f"{'🥇' if section == 'STOCKS' else '🏅'} *{section}:*\n"
        if results[section]:
            report += "`TKR   | PRICE  | D%    | W%    | SC`\n"
            # מיון לפי התשואה השבועית הגבוהה ביותר
            sorted_res = sorted(results[section], key=lambda x: x[3], reverse=True)[:15]
            for t, p, d, w, sc in sorted_res:
                report += f"`{t:<5} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {int(sc)}`\n"
        else:
            report += "_No high momentum found_\n"
        report += "\n"

    total = len(results["STOCKS"]) + len(results["ETF"])
    if total > 0:
        report += f"🚀 *Summary:* {total} Underdogs detected this week."
    else:
        report += "💡 *Summary:* No major weekly breakouts yet."

    send_msg(report)

if __name__ == "__main__":
    process_sentinel()
