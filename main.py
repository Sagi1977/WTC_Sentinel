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

# --- הגדרות ליבה ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

# הנבחרת שלך (Anchor & Turbo)
MY_PORTFOLIO = {
    "MBAV": {"score": 94.0}, "STKL": {"score": 94.0}, "ALF": {"score": 93.1},
    "ANAB": {"score": 84.9}, "KNSA": {"score": 84.0}
}

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "Markdown"}
    res = requests.post(url, json=payload)
    if res.status_code != 200:
        requests.post(url, json={"chat_id": CHAT_ID, "text": text[:4000]})
    time.sleep(1.2)

# --- גוגל דרייב ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_file(service, prefix):
    try:
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, f"❌ קובץ {prefix} לא נמצא"
        
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, f"✅ נטען: {prefix}"
    except: return None, f"❌ שגיאה בטעינת {prefix}"

# --- דאשבורד וניתוח ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p, s_p = vix['Close'].iloc[-1], spy['Close'].iloc[-1]
        s_c = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return f"📊 *WTC Sentinel Dashboard*\n`--------------------------`\n🚦 *Status:* `{status}` {emoji}\n📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n`--------------------------`\n"
    except: return "⚠️ Dashboard Offline\n\n"

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

def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX", "GC=F"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = custom_prompt if custom_prompt else f"""
    ענה בעברית כמחלקת מחקר של גולדמן סאקס. נתח חדשות: {news}
    בנה דוח בנקודות:
    ## דוח ניתוח שוק - תמונת מצב אסטרטגית
    ### 🏛️ 1. 'הכסף הגדול': מוסדיים ואסטרטגיה
    ### 💣 2. 'מוקשים ומאקרו': סיכונים וגיאופוליטיקה
    ### 🌡️ 3. 'סנטימנט השוק': שורה תחתונה לסוחר
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח החדשות."

# --- סריקה משולבת (STOCKS & ETF) ---
def run_execution_scan(service):
    results = {"STOCKS": [], "ETF": []}
    log = ""
    # השמות עם הקו התחתון כפי שהם בדרייב
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

    report = f"🎯 *WTC Execution Scan*\n{log}\n"
    report += f"🥇 *STOCKS Gold:* {', '.join(results['STOCKS']) or 'None'}\n"
    report += f"🏅 *ETF Gold:* {', '.join(results['ETF']) or 'None'}\n\n"
    
    if not results["STOCKS"] and not results["ETF"]:
        vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
        report += "💡 *סטטוס:* השוק בלחץ מכירות; אף נכס (כולל ה-ETF) לא הצליח לפרוץ את גבוה הבוקר." if vix > 22 else "💡 *סטטוס:* חוסר מומנטום כללי בפריצות."
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    db = get_market_dashboard()
    portfolio = get_portfolio_snapshot()

    if is_manual:
        send_telegram_msg(f"{db}{portfolio}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{db}{portfolio}\n{run_execution_scan(service)}")
    elif hour == 23:
        closing = "סכם בעברית את יום המסחר בוול סטריט עבור סוחר מקצועי. התייחס למדדים ולסגירה."
        send_telegram_msg(f"{db}🌙 *Closing Summary*\n\n{get_ai_report(closing)}")

if __name__ == "__main__":
    main()
