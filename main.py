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

# רשימת ה-Anchor וה-Turbo שלך כפי שמופיעים בדו"ח
MY_PORTFOLIO = {
    "MBAV": {"type": "⚓ Anchor", "score": 94.0},
    "STKL": {"type": "⚓ Anchor", "score": 94.0},
    "ALF":  {"type": "⚓ Anchor", "score": 93.1},
    "ANAB": {"type": "🚀 Turbo", "score": 84.9},
    "KNSA": {"type": "🚀 Turbo", "score": 84.0}
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
        res = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
        files = res.get('files', [])
        if not files: return None, f"קובץ {prefix} לא נמצא"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, "success"
    except: return None, "Error"

# --- מעקב פורטפוליו (ANCORE & TURBO) ---
def get_portfolio_snapshot():
    report = "📈 *My Portfolio Watch (Anchor & Turbo)*\n"
    report += "`Ticker | Price | Chg% | Status | Score`\n"
    report += "`---------------------------------------`\n"
    
    for ticker, info in MY_PORTFOLIO.items():
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="2d")
            data_5m = stock.history(period="1d", interval="5m")
            
            curr = hist['Close'].iloc[-1]
            prev = hist['Close'].iloc[-2]
            p_chg = ((curr / prev) - 1) * 100
            
            # בדיקת פריצה
            opening_high = data_5m.iloc[:6]['High'].max()
            status = "✅ Breakout" if curr > opening_high else "❌ Below"
            
            report += f"`{ticker:<5} | {curr:>6.2f} | {p_chg:>+5.1f}% | {status:<10} | {info['score']}`\n"
        except:
            report += f"`{ticker:<5} | N/A    | N/A    | Error      | {info['score']}`\n"
    
    return report + "`---------------------------------------`\n"

# --- דאשבורד וניתוח AI ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p, s_p = vix['Close'].iloc[-1], spy['Close'].iloc[-1]
        s_c = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return f"📊 *WTC Sentinel Dashboard*\n🚦 *Status:* `{status}` {emoji} | 📉 *VIX:* `{v_p:.2f}`\n\n"
    except: return "⚠️ Dashboard Offline\n\n"

def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = custom_prompt if custom_prompt else f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח."

# --- סריקת פריצות (Execution) ---
def run_execution_scan(service):
    results = {"Gold": [], "Underdogs": []}
    for prefix in ["GoldenPlanSTOCKS", "GoldenPlanETF"]:
        df, _ = download_latest_file(service, prefix)
        if df is not None and 'Ticker' in df.columns:
            for _, row in df.iterrows():
                t, s = str(row['Ticker']).strip(), row.get('Score', 0)
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) < 7: continue
                    if d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        if s >= 75: results["Gold"].append(f"{t}({s})")
                        elif s < 60: results["Underdogs"].append(f"{t}({s})")
                except: continue

    report = f"🎯 *WTC Execution Scan*\n"
    report += f"🥇 *Gold:* {', '.join(results['Gold']) or 'None'}\n"
    report += f"🐕 *Underdogs:* {', '.join(results['Underdogs']) or 'None'}\n\n"
    
    if not results["Gold"] and not results["Underdogs"]:
        vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
        report += "💡 *סטטוס:* השוק בלחץ/דשדוש; המניות נסחרות מתחת לגבוה היומי." if vix > 22 else "💡 *סטטוס:* חוסר מומנטום בפריצות."
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now.hour
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'

    db = get_market_dashboard()
    portfolio = get_portfolio_snapshot()

    if is_manual:
        send_telegram_msg(f"{db}{portfolio}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{db}{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{db}{portfolio}\n{run_execution_scan(service)}")
    elif hour == 23:
        closing = "סכם בעברית את יום המסחר בוול סטריט עבור סוחר מקצועי. התייחס למדדים ולסגירה."
        send_telegram_msg(f"{db}🌙 *Closing Summary*\n\n{get_ai_report(closing)}")

if __name__ == "__main__":
    main()
