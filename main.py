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
        if not files: return None, f"❓ {prefix} Not Found"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip() for c in df.columns]
        return df, "✅ Loaded"
    except Exception as e:
        return None, f"❌ Error: {str(e)}"

# --- 1. בניית רשימת מעקב דינמית (Anchor, Turbo, Top 5 ETF) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    prefixes = ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]
    for prefix in prefixes:
        df, status = download_latest_file(service, prefix)
        logs.append(f"{prefix}: {status}")
        if df is not None:
            # איתור עמודת ה-Selection (גמיש לשמות כמו Final_Selection)
            target_col = next((c for c in df.columns if 'Final' in c or 'Selection' in c), None)
            if target_col:
                # חיפוש המילים שאתה משתמש בהן בקובץ
                mask = df[target_col].str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    ticker = str(row['Ticker']).strip()
                    watchlist[ticker] = {
                        "type": str(row[target_col]).split('(')[0].strip()[:10],
                        "score": row.get('Score', 0)
                    }
                logs.append(f"🔍 Found {len(filtered)} items in {prefix}")
            else:
                logs.append(f"⚠️ Column 'Final_Selection' not found in {prefix}")
    return watchlist, "\n".join(logs)

# --- 2. ביצועי פורטפוליו (Day% / Wk%) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist is empty. Check your CSV labels.\n"
    
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    # חישוב יום שני ב-17:00 (ישראל)
    days_to_monday = now.weekday()
    monday_start = (now - datetime.timedelta(days=days_to_monday)).replace(hour=17, minute=0, second=0)
    
    report = "📈 *WTC Portfolio Watch (Dynamic)*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t, info in watchlist.items():
        try:
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
            # סטטוס פריצה
            o_high = today.iloc[:2]['High'].max() if len(today) >= 2 else curr
            stat = "✅ Brk" if curr >= o_high else "❌ Bel"
            report += f"`{t:<5} | {curr:>6.2f} | {d_chg:>+5.1f}% | {w_chg:>+5.1f}% | {stat}`\n"
        except: continue
    return report + "`---------------------------------------`\n"

# --- 3. ניתוח AI גולדמן סאקס ---
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = custom_prompt if custom_prompt else f"""
    ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}
    מבנה: ## דוח אסטרטגי
    ### 🏛️ 1. הכסף הגדול
    ### 💣 2. מוקשים ומאקרו
    ### 🌡️ 3. סנטימנט השוק
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ AI Offline"

# --- 4. סריקת פריצות ומשפט סיכום טכני ---
def run_execution_scan(service):
    results = {"STOCKS": [], "ETF": []}
    for prefix, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, prefix)
        if df is not None:
            for _, row in df.iterrows():
                t, s = str(row.get('Ticker', '')).strip(), row.get('Score', 0)
                if not t: continue
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) >= 7 and d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        results[label].append(f"{t}({s})")
                except: continue

    report = f"🎯 *WTC Execution Scan Result:*\n"
    report += f"🥇 *STOCKS Gold:* {', '.join(results['STOCKS']) or 'None'}\n"
    report += f"🏅 *ETF Gold:* {', '.join(results['ETF']) or 'None'}\n\n"
    
    # משפט הסיכום הטכני כפי שסיכמנו
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    if not results["STOCKS"] and not results["ETF"]:
        if vix > 22:
            report += "💡 *סיכום טכני:* השוק בלחץ מכירות ותנודתיות גבוהה; המניות נסחרות מתחת לגבוה היומי - מומלץ להמתין להרגעה ב-VIX."
        else:
            report += "💡 *סיכום טכני:* השוק בדשדוש או חוסר מומנטום; לא זוהו פריצות איכותיות מעל ה-Opening High."
    else:
        report += f"🚀 *סיכום טכני:* זוהו פריצות מומנטום ב-{len(results['STOCKS']) + len(results['ETF'])} נכסים. השוק מאפשר עסקאות פריצה."
    
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    # טעינה ודיווח
    watchlist, drive_logs = build_dynamic_watchlist(service)
    
    spy = yf.Ticker("SPY").history(period="2d")
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    s_p = spy['Close'].iloc[-1]
    s_c = ((s_p / spy['Close'].iloc[-2]) - 1) * 100
    
    db = f"📊 *WTC Sentinel Dashboard*\n`--------------------------`\n🚦 Status: `{'BEARISH' if vix > 25 else 'CAUTION'}` | VIX: `{vix:.2f}`\nSPY: `{s_p:.2f} ({s_c:+.2f}%)`\n\n`LOGS:`\n{drive_logs}\n`--------------------------`\n"

    performance = get_portfolio_performance(watchlist)

    if is_manual:
        send_telegram_msg(f"{db}\n{performance}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{db}\n{performance}\n{run_execution_scan(service)}")
    elif hour == 23:
        send_telegram_msg(f"{db}🌙 *Closing Summary*\n\n{get_ai_report('סכם את יום המסחר ואיך השוק נסגר.')}")

if __name__ == "__main__":
    main()
