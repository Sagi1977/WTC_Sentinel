import os
import time
import io
import requests
import pandas as pd
import yfinance as yf
import pytz
import datetime
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# הגדרות טלגרם - וודא שהן קיימות ב-Environment
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
    """מוצא את הקובץ האחרון בדרייב לפי תחילית השם"""
    try:
        query = f"name contains '{prefix}' and trashed = false"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get("files", [])
        if not files: return None
        
        file_id = files[0]['id']
        file_name = files[0]['name']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        # נירמול עמודות למניעת שגיאות Ticker/Symbol
        df.columns = [c.strip().lower() for c in df.columns]
        return df
    except Exception as e:
        print(f"Error downloading {prefix}: {e}")
        return None

def get_5m_rth(ticker, period="7d"):
    """מושך נתונים ומנקה MultiIndex של yfinance אם קיים"""
    try:
        df = yf.download(ticker, period=period, interval="5m", progress=False)
        if df.empty: return None
        
        # טיפול בשינויי מבנה של yfinance v0.2.x+
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        et_idx = df.index.tz_convert("America/New_York") if df.index.tz else df.index
        # סינון לשעות המסחר בלבד
        return df[(et_idx.hour > 9 or (et_idx.hour == 9 and et_idx.minute >= 30)) & (et_idx.hour < 16)]
    except: return None

def get_monday_10am_open(ticker):
    """לוגיקה מתוקנת: מציאת יום שני האחרון ב-10:00 בבוקר"""
    try:
        df = get_5m_rth(ticker, period="7d")
        if df is None or df.empty: return None
        
        et_idx = df.index.tz_convert("America/New_York")
        # מציאת ימי שני ומריחתם מהחדש לישן
        mondays = sorted(list(set([ts.date() for ts in et_idx if ts.weekday() == 0])), reverse=True)
        if not mondays: return None
        
        latest_monday_df = df[et_idx.date == mondays[0]]
        # מחפש מחיר פתיחה ב-10:00 בבוקר (שעון ניו יורק)
        target = latest_monday_df[latest_monday_df.index.tz_convert("America/New_York").time >= datetime.time(10, 0)]
        return target.iloc[0]['Open'] if not target.empty else latest_monday_df.iloc[0]['Open']
    except: return None

def process_sentinel():
    service = get_drive_service()
    
    # חיפוש היברידי: קודם גולדן, אם אין אז פריוריטי
    df_p = download_latest_file(service, "Golden_Plan")
    source = "Golden Plan"
    if df_p is None:
        df_p = download_latest_file(service, "Smart_Priority")
        source = "Smart Priority"
        
    if df_p is None:
        send_msg("❌ שגיאה: לא נמצא קובץ נתונים (Golden/Priority) בדרייב.")
        return

    # זיהוי עמודות חכם
    sym_col = 'symbol' if 'symbol' in df_p.columns else ('ticker' if 'ticker' in df_p.columns else None)
    score_col = 'final_score' if 'final_score' in df_p.columns else ('score' if 'score' in df_p.columns else None)
    
    if not sym_col:
        send_msg(f"❌ שגיאה: לא נמצאה עמודת טיקר בקובץ {source}.")
        return

    res = {"STOCKS": [], "ETF": []}
    tickers = df_p[sym_col].dropna().unique().tolist()
    
    try:
        v_p = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    except: v_p = 0

    for t in tickers:
        try:
            m_open = get_monday_10am_open(t)
            # נתוני היום הנוכחי
            df_now = yf.download(t, period="1d", interval="5m", progress=False)
            if df_now.empty or m_open is None: continue
            
            if isinstance(df_now.columns, pd.MultiIndex):
                df_now.columns = df_now.columns.get_level_values(0)
                
            cp = df_now['Close'].iloc[-1]
            day_open = df_now['Open'].iloc[0]
            
            d_chg = ((cp / day_open) - 1) * 100
            wk_chg = ((cp / m_open) - 1) * 100
            
            row = df_p[df_p[sym_col] == t].iloc[0]
            score = row.get(score_col, 0)
            is_etf = str(row.get('is_etf', 'false')).lower() == 'true'
            
            # תנאי האנדרדוג
            if wk_chg > 5 and score > 75:
                cat = "ETF" if is_etf else "STOCKS"
                res[cat].append((t, cp, d_chg, wk_chg, score))
        except: continue

    # בניית הדוח
    report = f"🛰️ *WTC Sentinel Dashboard* | VIX: `{v_p:.2f}`\n"
    report += f"📂 Source: `{source}`\n"
    report += "`-----------------------------`\n\n"
    
    found = 0
    for cat in ["STOCKS", "ETF"]:
        report += f"{'🥇' if cat == 'STOCKS' else '🏅'} *{cat}:*\n"
        if res[cat]:
            report += "`Ticker | Price | Day% | Wk% | Score`\n"
            report += "`-----------------------------------`\n"
            # מיון לפי תשואה שבועית (Wk%)
            sorted_data = sorted(res[cat], key=lambda x: x[3], reverse=True)
            for t, p, d, w, sc in sorted_data:
                report += f"`{t:<6} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {int(sc):<5}`\n"
                found += 1
        else:
            report += "_None_\n"
        report += "\n"

    if found > 0:
        report += f"🚀 *סיכום:* {found} אנדרדוגס בפריצה שבועית."
    else:
        report += "💡 *סיכום:* לא נמצאו פריצות מעל 5% כרגע."

    send_msg(report)

if __name__ == "__main__":
    process_sentinel()
