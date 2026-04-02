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

def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_file(service, prefix):
    try:
        # חיפוש גמיש מאוד
        query = f"name contains '{prefix}'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, f"❓ {prefix} Not Found"
        
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip() for c in df.columns] # ניקוי רווחים בשמות עמודות
        return df, f"✅ {prefix} Loaded"
    except Exception as e:
        return None, f"❌ Error {prefix}: {str(e)}"

# --- בניית רשימת מעקב עם "סורק עמודות" חכם ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    # ננסה את כל הוריאציות של השמות שלך
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF", "GoldenPlan"]:
        df, status = download_latest_file(service, prefix)
        logs.append(status)
        if df is not None:
            # מחפש עמודה שמכילה את המילה Final או Selection או Type
            target_col = next((c for c in df.columns if 'Final' in c or 'Selection' in c or 'selection' in c), None)
            ticker_col = next((c for c in df.columns if 'Ticker' in c or 'Symbol' in c), 'Ticker')
            
            if target_col:
                # סינון גמיש למילים שאתה משתמש בהן
                mask = df[target_col].str.contains('Anchor|Turbo|Top 5|Top 3', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    t = str(row[ticker_col]).strip()
                    watchlist[t] = {
                        "type": str(row[target_col]).split('(')[0].strip()[:10],
                        "score": row.get('Score', 0)
                    }
                logs.append(f"📊 Found {len(filtered)} items in {prefix}")
            else:
                logs.append(f"⚠️ Column 'Final_Selection' missing in {prefix}")
    return watchlist, "\n".join(logs)

# --- ביצועי פורטפוליו ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist is empty. Check CSV content.\n"
    
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_monday = now.weekday()
    monday_start = (now - datetime.timedelta(days=days_to_monday)).replace(hour=17, minute=0, second=0)
    
    report = "📈 *WTC Portfolio Watch*\n"
    report += "`Ticker | Price  | Day%  | Wk%   | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t, info in watchlist.items():
        try:
            # טעינת נתונים מורחבת לחישוב השבועי
            data = yf.download(t, start=(monday_start - datetime.timedelta(days=2)).strftime('%Y-%m-%d'), interval="15m", progress=False)
            if data.empty: continue
            
            curr = data['Close'].iloc[-1]
            # יום: מהפתיחה של היום (16:30)
            today = data[data.index.date == now.date()]
            d_open = today['Open'].iloc[0] if not today.empty else curr
            d_chg = ((curr / d_open) - 1) * 100
            
            # שבוע: מיום שני ב-17:00
            mon_data = data[data.index >= monday_start.strftime('%Y-%m-%d %H:%M:%S')]
            w_open = mon_data['Open'].iloc[0] if not mon_data.empty else d_open
            w_chg = ((curr / w_open) - 1) * 100
            
            # פריצה
            o_high = today.iloc[:2]['High'].max() if len(today) >= 2 else curr
            stat = "✅ Brk" if curr >= o_high else "❌ Bel"
            
            report += f"`{t:<6} | {curr:>6.2f} | {d_chg:>+5.1f}% | {w_chg:>+5.1f}% | {stat}`\n"
        except: continue
    return report + "`---------------------------------------`\n"

# --- AI ודוח גולדמן סאקס ---
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    p = custom_prompt if custom_prompt else f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except: return "⚠️ AI Offline"

def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, pref)
        if df is not None:
            for _, row in df.iterrows():
                t, s = str(row.get('Ticker', '')).strip(), row.get('Score', 0)
                if not t: continue
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) >= 7 and d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        res[label].append(f"{t}({s})")
                except: continue
    return f"🎯 *Execution Scan*\n🥇 STOCKS: {', '.join(res['STOCKS']) or 'None'}\n🏅 ETF: {', '.join(res['ETF']) or 'None'}\n"

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    # טעינה
    watchlist, drive_logs = build_dynamic_watchlist(service)
    
    # Dashboard
    spy = yf.Ticker("SPY").history(period="2d")
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    s_p = spy['Close'].iloc[-1]
    s_c = ((s_p / spy['Close'].iloc[-2]) - 1) * 100
    
    db = f"📊 *WTC Sentinel Dashboard*\n🚦 Status: `{'BEARISH' if vix > 25 else 'CAUTION'}` | VIX: `{vix:.2f}`\nSPY: `{s_p:.2f} ({s_c:+.2f}%)`\n\n`LOGS:`\n{drive_logs}\n"

    perf = get_portfolio_performance(watchlist)

    if is_manual:
        send_telegram_msg(f"{db}\n{perf}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{db}\n{perf}\n{run_execution_scan(service)}")
    elif hour == 23:
        send_telegram_msg(f"{db}🌙 *Closing Summary*\n\n{get_ai_report('סכם את יום המסחר ואיך השוק נסגר.')}")

if __name__ == "__main__":
    main()
