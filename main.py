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

# הגדרות טלגרם
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
        # חיפוש חכם: מתעלם מסל המחזור ומחפש קבצי CSV בלבד
        query = f"name contains '{prefix}' and trashed = false and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get("files", [])
        
        if not files:
            print(f"⚠️ לא נמצא קובץ עם הביטוי '{prefix}'")
            return None
            
        file_id = files[0]['id']
        print(f"✅ נמצא קובץ: {files[0]['name']}")
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    except Exception as e:
        print(f"❌ שגיאה בהורדה: {str(e)}")
        return None

def get_latest_monday_open(ticker_data):
    """תיקון באג יום שני: מוצא את יום שני האחרון בנתונים"""
    try:
        if ticker_data.empty: return None
        et_idx = ticker_data.index.tz_convert("America/New_York")
        mondays = sorted(list(set([ts.date() for ts in et_idx if ts.weekday() == 0])), reverse=True)
        if not mondays: return None
        
        target_date = mondays[0] # יום שני הכי קרוב להיום
        monday_df = ticker_data[et_idx.date == target_date]
        
        for hour in [10, 11, 12]:
            target = monday_df.between_time(f"{hour}:00", f"{hour}:15")
            if not target.empty: return target.iloc[0]['Open']
        return monday_df.iloc[0]['Open']
    except: return None

def process_sentinel():
    service = get_drive_service()
    
    # שימוש ב-Prefix המדויק שציינת
    df_p = download_latest_file(service, "Smart_Priority")
    if df_p is None:
        send_msg("❌ שגיאה: לא נמצא קובץ Smart_Priority בדרייב. וודא שהקובץ קיים בתיקייה 2.")
        return

    # ניקוי רשימת הטיקרים
    tickers = df_p['symbol'].dropna().unique().tolist()
    
    print(f"🚀 מוריד נתונים עבור {len(tickers)} מניות...")
    # הורדה קבוצתית לביצועים מהירים
    full_data = yf.download(tickers, period="7d", interval="5m", group_by='ticker', threads=True)
    
    # מדד ה-VIX
    try:
        vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    except: vix = 0
    
    results = {"STOCKS": [], "ETF": []}
    
    for ticker in tickers:
        try:
            t_data = full_data[ticker].dropna()
            if t_data.empty: continue
            
            curr_price = t_data['Close'].iloc[-1]
            
            # חישוב שינוי יומי (מפתיחת היום)
            today_data = t_data[t_data.index.date == t_data.index[-1].date()]
            day_open = today_data['Open'].iloc[0]
            day_chg = ((curr_price / day_open) - 1) * 100
            
            # חישוב שינוי שבועי (מתחילת יום שני האחרון - תיקון הבאג)
            monday_open = get_latest_monday_open(t_data)
            wk_chg = ((curr_price / monday_open) - 1) * 100 if monday_open else 0
            
            score = df_p[df_p['symbol'] == ticker]['final_score'].values[0]
            is_etf = df_p[df_p['symbol'] == ticker]['is_etf'].values[0]
            
            # פילטר ה-UnderDogs (צמיחה שבועית חזקה וציון איכות)
            if wk_chg > 5 and score > 75:
                cat = "ETF" if is_etf else "STOCKS"
                results[cat].append((ticker, curr_price, day_chg, wk_chg, score))
        except: continue

    # בניית הדוח לטלגרם
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
            report += "_לא נמצאו פריצות שבועיות_\n"
        report += "\n"

    total = len(results["STOCKS"]) + len(results["ETF"])
    if total > 0:
        report += f"🚀 *סיכום:* {total} מניות אנדרדוג זוהו השבוע."
    else:
        report += "💡 *סיכום:* אין כרגע פריצות שבועיות משמעותיות."

    send_msg(report)

if __name__ == "__main__":
    process_sentinel()
