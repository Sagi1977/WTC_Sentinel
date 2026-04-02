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
        if not files: return None, "Missing"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, "Success"
    except: return None, "Error"

# --- 1. בניית רשימת נכסי המפתח (Dynamic Watchlist) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    prefixes = ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]
    for prefix in prefixes:
        df, _ = download_latest_file(service, prefix)
        if df is not None:
            # איתור העמודה הנכונה
            target_col = next((c for c in df.columns if 'Final' in c), None)
            if target_col:
                # סינון לפי מילות המפתח שהגדרת ב-CSV
                filtered = df[df[target_col].str.contains('Anchor|Turbo|Top 5', na=False, case=False)]
                for _, row in filtered.iterrows():
                    ticker = str(row['Ticker']).strip()
                    watchlist[ticker] = {
                        "type": str(row[target_col]).split('(')[0].strip(), # ניקוי השם
                        "score": row.get('Score', 0)
                    }
    return watchlist

# --- 2. דאשבורד ופורטפוליו ---
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

def get_portfolio_snapshot(watchlist):
    if not watchlist: return "⚠️ לא נמצאו נכסי מפתח בקבצים.\n"
    report = "📈 *My Portfolio Watch (Dynamic)*\n"
    report += "`Type       | Ticker | Price | Chg% | Status`\n"
    for t, info in watchlist.items():
        try:
            s = yf.Ticker(t)
            h = s.history(period="2d")
            d5 = s.history(period="1d", interval="5m")
            curr = h['Close'].iloc[-1]
            p_chg = ((curr / h['Close'].iloc[-2]) - 1) * 100
            oh = d5.iloc[:6]['High'].max()
            stat = "✅ Break" if curr > oh else "❌ Below"
            report += f"`{info['type'][:10]:<10} | {t:<5} | {curr:>6.2f} | {p_chg:>+5.1f}% | {stat:<7}`\n"
        except: continue
    return report + "\n"

# --- 3. ניתוח AI מוסדי ---
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

# --- 4. סריקת פריצות (Execution) ---
def run_execution_scan(service):
    results = {"STOCKS": [], "ETF": []}
    log = ""
    mapping = {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}
    
    for prefix, label in mapping.items():
        df, status = download_latest_file(service, prefix)
        log += f"{prefix}: {status}\n"
        if df is not None and 'Ticker' in df.columns:
            for _, row in df.iterrows():
                t, s = str(row['Ticker']).strip(), row.get('Score', 0)
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) < 7: continue
                    if d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        results[label].append(f"{t}({s})")
                except: continue

    res_msg = f"🎯 *WTC Execution Scan Result:*\n{log}\n"
    res_msg += f"🥇 *STOCKS Gold:* {', '.join(results['STOCKS']) or 'None'}\n"
    res_msg += f"🏅 *ETF Gold:* {', '.join(results['ETF']) or 'None'}\n\n"
    
    if not results["STOCKS"] and not results["ETF"]:
        v_val = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
        res_msg += "💡 *סטטוס:* השוק בלחץ; המניות וה-ETF נסחרים מתחת לגבוה היומי." if v_val > 22 else "💡 *סטטוס:* חוסר מומנטום בפריצות."
    return res_msg

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    # שלב הדינמיות: בניית רשימת המעקב מהקבצים
    dynamic_watchlist = build_dynamic_watchlist(service)
    
    db = get_market_dashboard()
    portfolio = get_portfolio_snapshot(dynamic_watchlist)

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
