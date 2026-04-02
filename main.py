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

# הגדרת הנבחרת שלך (Anchor & Turbo) כפי שמופיעה בדו"ח
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

# --- גוגל דרייב - חיבור ואיתור קבצים ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_file(service, prefix):
    try:
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
        files = res.get('files', [])
        if not files:
            res = service.files().list(q=f"name contains '{prefix}'", orderBy="createdTime desc").execute()
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

# --- 1. דאשבורד שוק ---
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

# --- 2. מעקב פורטפוליו (Anchor & Turbo) ---
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
            p_chg = ((curr / hist['Close'].iloc[-2]) - 1) * 100
            opening_high = data_5m.iloc[:6]['High'].max()
            status = "✅ Breakout" if curr > opening_high else "❌ Below"
            report += f"`{ticker:<5} | {curr:>6.2f} | {p_chg:>+5.1f}% | {status:<10} | {info['score']}`\n"
        except:
            report += f"`{ticker:<5} | N/A    | N/A    | Error      | {info['score']}`\n"
    return report + "`---------------------------------------`\n"

# --- 3. ניתוח AI מוסדי ---
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX", "GC=F"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = custom_prompt if custom_prompt else f"""
    ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}
    מבנה הדוח:
    ## דוח ניתוח שוק - תמונת מצב אסטרטגית
    ### 🏛️ 1. 'הכסף הגדול': מוסדיים ואסטרטגיה
    ### 💣 2. 'מוקשים ומאקרו': סיכונים וגיאופוליטיקה
    ### 🌡️ 3. 'סנטימנט השוק': שורה תחתונה לסוחר
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח."

# --- 4. סריקת פריצות (Execution) ---
def run_execution_scan(service):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    if now.hour < 16 or (now.hour == 16 and now.minute < 30): return "🛑 *Market Closed.*"
    
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

    report = f"🎯 *WTC Execution Scan*\n🥇 *Gold:* {', '.join(results['Gold']) or 'None'}\n🐕 *Underdogs:* {', '.join(results['Underdogs']) or 'None'}\n\n"
    
    if not results["Gold"] and not results["Underdogs"]:
        vix_val = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
        if vix_val > 22:
            report += "💡 *סטטוס:* השוק בלחץ מכירות; המניות ברשימה נסחרות מתחת לגבוה היומי - מומלץ להמתין להרגעה ב-VIX."
        else:
            report += "💡 *סטטוס:* חוסר מומנטום בפריצות מעל ה-Opening High."
    return report

# --- MAIN - ניהול זמן ותזמון ---
def main():
    try:
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
            closing_p = "סכם בעברית את יום המסחר בוול סטריט עבור סוחר מקצועי. התייחס למדדים ולסגירה."
            send_telegram_msg(f"{db}🌙 *Closing Summary*\n\n{get_ai_report(closing_p)}")
            
    except Exception as e:
        send_telegram_msg(f"⚠️ שגיאה כללית: {str(e)}")

if __name__ == "__main__":
    main()
