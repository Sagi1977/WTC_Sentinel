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
        df.columns = [c.strip() for c in df.columns]
        return df, f"✅ {prefix} Loaded"
    except Exception as e:
        return None, f"❌ Error: {str(e)[:50]}"

# --- 1. בניית רשימת מעקב דינמית (Anchor, Turbo, Top 5 ETF) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        logs.append(status)
        if df is not None:
            # איתור עמודת ה-Selection (גמיש לשם העמודה בקובץ שלך)
            target_col = next((c for c in df.columns if 'Final' in c or 'Selection' in c), None)
            if target_col:
                mask = df[target_col].str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    ticker = str(row['Ticker']).strip()
                    watchlist[ticker] = {
                        "type": str(row[target_col]).split('(')[0].strip()[:10],
                        "score": row.get('Score', 0)
                    }
                logs.append(f"📊 Found {len(filtered)} items in {prefix}")
    return watchlist, "\n".join(logs)

# --- 2. ביצועי פורטפוליו (Day% / Wk% / Status) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist is empty. Check CSV labels.\n"
    
    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    # חישוב יום שני האחרון בשעה 17:00 (זמן ישראל)
    days_to_monday = now_isr.weekday()
    monday_start = (now_isr - datetime.timedelta(days=days_to_monday)).replace(hour=17, minute=0, second=0)
    
    report = "📈 *WTC Portfolio Watch (Dynamic)*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t, info in watchlist.items():
        try:
            data = yf.download(t, start=(monday_start - datetime.timedelta(days=2)).strftime('%Y-%m-%d'), interval="15m", progress=False)
            if data.empty: continue
            curr = data['Close'].iloc[-1]
            # יום: מהפתיחה של היום (16:30)
            today = data[data.index.date == now_isr.date()]
            d_open = today['Open'].iloc[0] if not today.empty else curr
            d_chg = ((curr / d_open) - 1) * 100
            # שבוע: מיום שני ב-17:00
            mon_data = data[data.index >= monday_start.strftime('%Y-%m-%d %H:%M:%S')]
            w_open = mon_data['Open'].iloc[0] if not mon_data.empty else d_open
            w_chg = ((curr / w_open) - 1) * 100
            # פריצה טכנית (Opening High)
            o_high = today.iloc[:2]['High'].max() if len(today) >= 2 else curr
            stat = "✅ Brk" if curr >= o_high else "❌ Bel"
            report += f"`{t:<5} | {curr:>6.2f} | {d_chg:>+5.1f}% | {w_chg:>+5.1f}% | {stat}`\n"
        except: continue
    return report + "`---------------------------------------`\n"

# --- 3. ניתוח AI מוסדי (Gemini) ---
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = custom_prompt if custom_prompt else f"""
    ענה בעברית כמחלקת מחקר של גולדמן סאקס. נתח חדשות: {news}
    בנה דוח בנקודות:
    ## דוח אסטרטגי - תמונת מצב מוסדית
    ### 🏛️ 1. הכסף הגדול: מוסדיים ואסטרטגיה
    ### 💣 2. מוקשים ומאקרו: סיכונים וגיאופוליטיקה
    ### 🌡️ 3. סנטימנט השוק: שורה תחתונה לסוחר
    """
    try:
        if not GEMINI_KEY: return "❌ מפתח ה-API חסר ב-Secrets!"
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except Exception as e:
        return f"⚠️ שגיאת AI טכנית: {str(e)[:100]}"

# --- 4. סריקת פריצות וסיכום טכני ---
def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, pref)
        if df is not None:
            for _, row in df.iterrows():
                t, s = str(row.get('Ticker', '')).strip(), row.get('Score', 0)
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) >= 7 and d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        res[label].append(f"{t}({s})")
                except: continue

    report = f"🎯 *WTC Execution Scan Result:*\n🥇 STOCKS Gold: {', '.join(res['STOCKS']) or 'None'}\n🏅 ETF Gold: {', '.join(res['ETF']) or 'None'}\n\n"
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    
    if not res["STOCKS"] and not res["ETF"]:
        if vix > 22:
            report += "💡 *סיכום טכני:* השוק בלחץ מכירות ותנודתיות גבוהה; המניות נסחרות מתחת לגבוה היומי - מומלץ להמתין להרגעה ב-VIX."
        else:
            report += "💡 *סיכום טכני:* השוק בדשדוש או חוסר מומנטום; לא זוהו פריצות איכותיות מעל ה-Opening High."
    else:
        report += f"🚀 *סיכום טכני:* זוהו פריצות ב-{len(res['STOCKS']) + len(res['ETF'])} נכסים."
    return report

# --- 5. דאשבורד שוק ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p, s_p = vix['Close'].iloc[-1], spy['Close'].iloc[-1]
        s_c = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        return f"📊 *WTC Sentinel Dashboard*\n`--------------------------`\n🚦 Status: `{'BEARISH' if v_p > 25 else 'CAUTION' if v_p > 18 else 'BULLISH'}` | 📉 VIX: `{v_p:.2f}`\n📈 SPY: `{s_p:.2f} ({s_c:+.2f}%)`\n"
    except: return "⚠️ Dashboard Offline\n\n"

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    # טעינת נתונים
    watchlist, drive_logs = build_dynamic_watchlist(service)
    db = get_market_dashboard()
    performance = get_portfolio_performance(watchlist)

    if is_manual:
        send_telegram_msg(f"{db}\n`LOGS:`\n{drive_logs}\n`--------------------------`\n{performance}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16: # פתיחת מסחר
        send_telegram_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour < 23: # זמן ביצוע (Execution)
        send_telegram_msg(f"{db}\n{performance}\n{run_execution_scan(service)}")
    elif hour == 23: # סגירת מסחר
        closing_msg = "סכם בעברית את יום המסחר בוול סטריט עבור סוחר מקצועי. התייחס למדדים, תנודתיות ואיך השוק נסגר."
        send_telegram_msg(f"{db}🌙 *WTC Closing Summary*\n\n{get_ai_report(closing_msg)}")

if __name__ == "__main__":
    main()
