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
        # חיפוש גמיש שמתעלם מקווים תחתיים או רווחים
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, f"❌ קובץ {prefix} לא נמצא בדרייב"
        
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        # ניקוי שמות עמודות מרווחים
        df.columns = [c.strip() for c in df.columns]
        return df, f"✅ {prefix} נטען"
    except Exception as e:
        return None, f"⚠️ שגיאה בטעינת {prefix}: {str(e)}"

# --- 1. בניית רשימת מעקב דינמית מה-CSV ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    # בדיקת שני הפורמטים האפשריים לשמות הקבצים שלך
    prefixes = ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]
    
    for prefix in prefixes:
        df, status = download_latest_file(service, prefix)
        logs.append(status)
        if df is not None:
            # חיפוש עמודת ה-Selection (גמיש לשם העמודה)
            target_col = next((c for c in df.columns if 'Final' in c or 'Selection' in c), None)
            if target_col:
                # מחפש מניות שסומנו כ-Anchor, Turbo או Top 5
                filtered = df[df[target_col].str.contains('Anchor|Turbo|Top 5', na=False, case=False)]
                for _, row in filtered.iterrows():
                    ticker = str(row['Ticker']).strip()
                    watchlist[ticker] = {
                        "type": str(row[target_col]).split('(')[0].strip(),
                        "score": row.get('Score', 0)
                    }
            else:
                logs.append(f"⚠️ לא נמצאה עמודת Selection ב-{prefix}")
    return watchlist, "\n".join(logs)

# --- 2. ביצועי פורטפוליו (יומי ושבועי) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ רשימת המעקב ריקה (בדוק את הקבצים בדרייב).\n"
    
    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    # יום שני האחרון ב-17:00
    days_to_monday = now_isr.weekday()
    monday_start = (now_isr - datetime.timedelta(days=days_to_monday)).replace(hour=17, minute=0, second=0, microsecond=0)
    
    report = "📈 *WTC Portfolio Watch (Dynamic)*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t, info in watchlist.items():
        try:
            # הורדת נתונים מפורטת
            data = yf.download(t, start=(monday_start - datetime.timedelta(days=2)).strftime('%Y-%m-%d'), interval="15m", progress=False)
            if data.empty: continue
            
            curr_p = data['Close'].iloc[-1]
            
            # שינוי מהפתיחה היום (16:30)
            today_data = data[data.index.date == now_isr.date()]
            day_open = today_data['Open'].iloc[0] if not today_data.empty else curr_p
            day_chg = ((curr_p / day_open) - 1) * 100
            
            # שינוי משני ב-17:00
            monday_data = data[data.index >= monday_start.strftime('%Y-%m-%d %H:%M:%S')]
            week_open = monday_data['Open'].iloc[0] if not monday_data.empty else day_open
            week_chg = ((curr_p / week_open) - 1) * 100
            
            # סטטוס פריצה (Opening High - 30 דקות ראשונות)
            opening_high = today_data.iloc[:2]['High'].max() if len(today_data) >= 2 else curr_p
            status = "✅ Brk" if curr_p >= opening_high else "❌ Bel"
            
            report += f"`{t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {week_chg:>+5.1f}% | {status}`\n"
        except: continue
    return report + "`---------------------------------------`\n"

# --- 3. ניתוח AI מוסדי ---
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = custom_prompt if custom_prompt else f"""
    ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח חדשות: {news}
    מבנה: ## דוח אסטרטגי
    ### 🏛️ 1. הכסף הגדול
    ### 💣 2. מוקשים ומאקרו
    ### 🌡️ 3. סנטימנט השוק
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח."

# --- 4. סריקת פריצות (Execution Scan) ---
def run_execution_scan(service):
    results = {"STOCKS": [], "ETF": []}
    mapping = {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}
    
    for prefix, label in mapping.items():
        df, _ = download_latest_file(service, prefix)
        if df is not None and 'Ticker' in df.columns:
            for _, row in df.iterrows():
                t, s = str(row['Ticker']).strip(), row.get('Score', 0)
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) < 7: continue
                    if d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        results[label].append(f"{t}({s})")
                except: continue

    report = f"🎯 *Execution Scan Result:*\n"
    report += f"🥇 *STOCKS:* {', '.join(results['STOCKS']) or 'None'}\n"
    report += f"🏅 *ETF:* {', '.join(results['ETF']) or 'None'}\n\n"
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    # טעינה דינמית ודיווח לוגים
    watchlist, drive_logs = build_dynamic_watchlist(service)
    
    # דאשבורד
    spy = yf.Ticker("SPY").history(period="2d")
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    s_p = spy['Close'].iloc[-1]
    s_c = ((s_p / spy['Close'].iloc[-2]) - 1) * 100
    db = f"📊 *WTC Sentinel Dashboard*\n🚦 Status: `{'BEARISH' if vix > 25 else 'CAUTION'}` | VIX: `{vix:.2f}`\nSPY: `{s_p:.2f} ({s_c:+.2f}%)`\n\n`LOGS:`\n{drive_logs}\n`--------------------------`\n"

    performance = get_portfolio_performance(watchlist)

    if is_manual:
        send_telegram_msg(f"{db}{performance}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{db}{performance}\n{run_execution_scan(service)}")
    elif hour == 23:
        closing = "סכם בעברית את יום המסחר בוול סטריט עבור סוחר מקצועי. התייחס למדדים ולסגירה."
        send_telegram_msg(f"{db}🌙 *Closing Summary*\n\n{get_ai_report(closing)}")

if __name__ == "__main__":
    main()
