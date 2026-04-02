import os
import datetime
import time
import pandas as pd
import yfinance as yf
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai
import google.auth
import io

# --- הגדרות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

# הנבחרת שלך
MY_PORTFOLIO = {
    "MBAV": {"score": 94.0}, "STKL": {"score": 94.0}, "ALF": {"score": 93.1},
    "ANAB": {"score": 84.9}, "KNSA": {"score": 84.0}
}

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "Markdown"})
    time.sleep(1)

def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_file(service, prefix):
    try:
        # שימוש ב-contains כדי להיות גמיש לקו תחתון או רווח
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, f"❌ {prefix} לא נמצא"
        
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, f"✅ נטען {prefix}"
    except: return None, f"❌ שגיאה ב-{prefix}"

def get_portfolio_snapshot():
    report = "📈 *My Portfolio Watch (Anchor & Turbo)*\n"
    report += "`Ticker | Price | Chg% | Status | Score`\n"
    for t, info in MY_PORTFOLIO.items():
        try:
            s = yf.Ticker(t)
            h = s.history(period="2d")
            d5 = s.history(period="1d", interval="5m")
            curr = h['Close'].iloc[-1]
            p_chg = ((curr / h['Close'].iloc[-2]) - 1) * 100
            oh = d5.iloc[:6]['High'].max()
            stat = "✅ Breakout" if curr > oh else "❌ Below"
            report += f"`{t:<5} | {curr:>6.2f} | {p_chg:>+5.1f}% | {stat:<10} | {info['score']}`\n"
        except: continue
    return report + "\n"

def run_execution_scan(service):
    results = {"STOCKS": [], "ETF": []}
    log = ""
    # השמות המדויקים כפי שהם בדרייב שלך
    mapping = {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}
    
    for prefix, label in mapping.items():
        df, status = download_latest_file(service, prefix)
        log += f"{status}\n"
        if df is not None and 'Ticker' in df.columns:
            for _, row in df.iterrows():
                t, s = str(row['Ticker']).strip(), row.get('Score', 0)
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) < 7: continue
                    if d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        results[label].append(f"{t}({s})")
                except: continue

    res_msg = f"🎯 *WTC Execution Scan*\n{log}\n"
    res_msg += f"🥇 *STOCKS Gold:* {', '.join(results['STOCKS']) or 'None'}\n"
    res_msg += f"🏅 *ETF Gold:* {', '.join(results['ETF']) or 'None'}\n"
    return res_msg

def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    
    spy = yf.Ticker("SPY").history(period="2d")
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    s_p = spy['Close'].iloc[-1]
    s_c = ((s_p / spy['Close'].iloc[-2]) - 1) * 100
    
    header = f"📊 *WTC Sentinel Dashboard*\n`--------------------------`\n🚦 *Status:* `{'BEARISH' if vix > 25 else 'CAUTION'}`\n📉 *VIX:* `{vix:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n`--------------------------`\n"
    
    portfolio = get_portfolio_snapshot()
    scan = run_execution_scan(service)
    
    # שליחת הכל כהודעה אחת מרכזית שלא תפספס כלום
    full_report = f"{header}\n{portfolio}\n{scan}"
    send_telegram_msg(full_report)

if __name__ == "__main__":
    main()
