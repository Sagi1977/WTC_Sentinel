import os
import time
import io
import requests
import pandas as pd
import yfinance as yf
import datetime
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# הגדרות מערכת
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
BASE = f"https://api.telegram.org/bot{TOKEN}"

def send_msg(text):
    if not text: return
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(f"{BASE}/sendMessage", 
                         json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "Markdown"}, 
                         timeout=15)
        except: pass
        time.sleep(0.5)

def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)

def download_latest_file(service, prefix):
    try:
        query = f"name contains '{prefix}' and trashed = false"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get("files", [])
        if not files: return None
        
        request = service.files().get_media(fileId=files[0]['id'])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().lower() for c in df.columns]
        return df
    except: return None

def get_clean_data(ticker, period="7d"):
    """מושך נתונים ומנקה כותרות MultiIndex"""
    try:
        df = yf.download(ticker, period=period, interval="5m", progress=False)
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except: return None

def get_monday_open(ticker):
    """מוצא את מחיר הפתיחה של יום שני האחרון ב-10:00 בבוקר"""
    try:
        df = get_clean_data(ticker, period="7d")
        if df is None: return None
        et_idx = df.index.tz_convert("America/New_York") if df.index.tz else df.index
        mondays = sorted(list(set([ts.date() for ts in et_idx if ts.weekday() == 0])), reverse=True)
        if not mondays: return None
        
        monday_df = df[et_idx.date == mondays[0]]
        target = monday_df[monday_df.index.tz_convert("America/New_York").time >= datetime.time(10, 0)]
        return target.iloc[0]['Open'] if not target.empty else monday_df.iloc[0]['Open']
    except: return None

def process_sentinel():
    service = get_drive_service()
    
    # חיפוש היברידי
    df_p = download_latest_file(service, "Golden_Plan")
    source = "Golden Plan"
    if df_p is None:
        df_p = download_latest_file(service, "Smart_Priority")
        source = "Priority"
        
    if df_p is None:
        send_msg("❌ שגיאה: לא נמצא קובץ נתונים בדרייב.")
        return

    sym_col = 'symbol' if 'symbol' in df_p.columns else ('ticker' if 'ticker' in df_p.columns else None)
    score_col = 'final_score' if 'final_score' in df_p.columns else ('score' if 'score' in df_p.columns else None)
    
    if not sym_col: return

    res = {"STOCKS": [], "ETF": []}
    tickers = df_p[sym_col].dropna().unique().tolist()
    
    try:
        vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    except: vix = 0

    for t in tickers:
        try:
            m_open = get_monday_open(t)
            df_now = get_clean_data(t, period="1d")
            if df_now is None or m_open is None: continue
            
            cp = df_now['Close'].iloc[-1]
            d_chg = ((cp / df_now['Open'].iloc[0]) - 1) * 100
            wk_chg = ((cp / m_open) - 1) * 100
            
            row = df_p[df_p[sym_col] == t].iloc[0]
            score = row.get(score_col, 0)
            is_etf = str(row.get('is_etf', 'false')).lower() == 'true'
            
            if wk_chg > 5 and score > 75:
                cat = "ETF" if is_etf else "STOCKS"
                res[cat].append((t, cp, d_chg, wk_chg, score))
        except: continue

    # תבנית עיצוב קבועה ליישור מושלם
    # TKR (6) | PRICE (6) | D% (5) | W% (5) | SC (3)
    fmt = "`{:<5} | {:>6.2f} | {:>+5.1f}% | {:>+5.1f}% | {:>3.0f}`"
    header = "`TKR   | PRICE  | D%    | W%    | SC `\n`-----------------------------------`"

    report = f"🛰️ *WTC Sentinel Dashboard* | VIX: `{vix:.2f}`\n"
    report += f"📂 Source: `{source}`\n\n"
    
    found = 0
    for cat in ["STOCKS", "ETF"]:
        report += f"{'🥇' if cat == 'STOCKS' else '🏅'} *{cat}:*\n"
        if res[cat]:
            report += header + "\n"
            for t, p, d, w, sc in sorted(res[cat], key=lambda x: x[3], reverse=True)[:15]:
                report += fmt.format(t, p, d, w, sc) + "\n"
                found += 1
        else:
            report += "_None_\n"
        report += "\n"

    report += f"🚀 *Summary:* {found} Underdogs detected." if found > 0 else "💡 *Summary:* No major breakouts."
    send_msg(report)

if __name__ == "__main__":
    process_sentinel()
