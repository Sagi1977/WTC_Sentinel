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
import re

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
        if not files: return None, f"❓ {prefix} Missing"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python')
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

# --- 1. בניית רשימת מעקב דינמית (חסינת שמות עמודות) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            # ניקוי שמות עמודות בצורה אגרסיבית
            df.columns = [re.sub(r'[^a-zA-Z]', '', str(c)).lower() for c in df.columns]
            
            # חיפוש עמודה שמכילה 'final' או 'selection'
            sel_col = next((c for c in df.columns if 'final' in c or 'selection' in c), None)
            ticker_col = next((c for c in df.columns if 'ticker' in c), 'ticker')
            
            if sel_col:
                # חיפוש חופשי של Anchor, Turbo, Top 5
                mask = df[sel_col].astype(str).str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    t = str(row[ticker_col]).strip().upper()
                    watchlist[t] = str(row[sel_col])
                logs.append(f"✅ {prefix}: מצאתי {len(filtered)} נכסים")
            else:
                logs.append(f"⚠️ {prefix}: עמודת Selection לא זוהתה! עמודות: {list(df.columns)[:5]}")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# --- 2. ביצועי פורטפוליו (Day% מ-16:30 / Wk% מיום שני 17:00) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist empty - Check CSV content.\n"
    
    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_monday = now_isr.weekday()
    monday_date = (now_isr - datetime.timedelta(days=days_to_monday)).date()
    
    report = "📈 *WTC Portfolio Watch (Dynamic)*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t in watchlist.keys():
        try:
            # נתונים מפורטים (15 דקות)
            data = yf.download(t, start=monday_date.strftime('%Y-%m-%d'), interval="15m", progress=False)
            if data.empty: continue
            
            curr_p = data['Close'].iloc[-1]
            
            # Day% - שינוי מפתיחת היום (16:30 ישראל = 13:30 UTC)
            today_data = data[data.index.date == now_isr.date()]
            day_open = today_data['Open'].iloc[0] if not today_data.empty else curr_p
            day_chg = ((curr_p / day_open) - 1) * 100
            
            # Wk% - שינוי מיום שני ב-17:00 (17:00 ישראל = 14:00 UTC)
            mon_data = data[(data.index.date == monday_date) & (data.index.hour >= 14)]
            # מחיר "קנייה": פתיחת הנר הראשון אחרי 17:00
            wk_open_price = mon_data['Open'].iloc[0] if not mon_data.empty else data['Open'].iloc[0]
            wk_chg = ((curr_p / wk_open_price) - 1) * 100
            
            # Status - פריצת גבוה הבוקר (30 דקות ראשונות)
            o_high = today_data.iloc[:2]['High'].max() if len(today_data) >= 2 else curr_p
            status = "✅ Brk" if curr_p >= o_high else "❌ Bel"
            
            report += f"`{t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
        except: continue
    return report + "`---------------------------------------`\n"

# --- 3. ניתוח AI גולדמן סאקס ---
def get_ai_report(custom_prompt=None):
    try:
        news = ""
        for t in ["^GSPC", "^VIX"]:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        p = custom_prompt if custom_prompt else f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except Exception as e: return f"⚠️ AI Error: {str(e)[:40]}"

# --- 4. סריקת פריצות וסיכום טכני ---
def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, prefix=pref)
        if df is not None:
            t_col = next((c for c in df.columns if 'ticker' in c.lower()), 'ticker')
            for _, row in df.iterrows():
                t = str(row.get(t_col, '')).strip()
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) >= 7 and d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        res[label].append(t)
                except: continue

    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    report = f"🎯 *Scan Result:*\n🥇 STOCKS Gold: {', '.join(res['STOCKS']) or 'None'}\n🏅 ETF Gold: {', '.join(res['ETF']) or 'None'}\n\n"
    if not res["STOCKS"] and not res["ETF"]:
        report += "💡 *סיכום טכני:* אין פריצות; השוק בדשדוש. " + ("להמתין ל-VIX." if vix > 22 else "")
    else:
        report += f"🚀 *סיכום טכני:* זוהו פריצות ב-{len(res['STOCKS']) + len(res['ETF'])} נכסים."
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    watchlist, drive_logs = build_dynamic_watchlist(service)
    spy = yf.Ticker("SPY").history(period="2d")
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    s_p = spy['Close'].iloc[-1]
    s_c = ((s_p / spy['Close'].iloc[-2]) - 1) * 100
    
    header = f"📊 *WTC Sentinel Dashboard*\n🚦 Status: `{'BEARISH' if vix > 25 else 'CAUTION'}` | VIX: `{vix:.2f}`\nSPY: `{s_p:.2f} ({s_c:+.2f}%)`\n\n🔍 *Diagnostics:*\n`{drive_logs}`\n"
    perf = get_portfolio_performance(watchlist)

    if is_manual:
        send_telegram_msg(f"{header}\n{perf}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{header}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{header}\n{perf}\n{run_execution_scan(service)}")
    elif hour == 23:
        send_telegram_msg(f"{header}🌙 *Closing Summary*\n\n{get_ai_report('סכם את יום המסחר.')}")

if __name__ == "__main__":
    main()
