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
        if not files: return None, f"❓ {prefix} Missing"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        # טעינה עם utf-8-sig לטיפול באמוג'ים
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python')
        df.columns = [str(c).strip() for c in df.columns]
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

# --- 1. בניית רשימת מעקב (זיהוי אגרסיבי וחסין) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            # מציאת עמודת ה-Selection (גמיש ל-Final_Selection, Selection וכו')
            sel_col = next((c for c in df.columns if 'final' in c.lower() or 'selection' in c.lower()), None)
            ticker_col = next((c for c in df.columns if 'ticker' in c.lower()), 'Ticker')
            
            if sel_col:
                # ניקוי הטקסט בעמודה לצורך חיפוש נקי
                df['clean_sel'] = df[sel_col].astype(str).str.replace(r'[^\w\s]', '', regex=True).str.lower()
                # חיפוש המילים שלך
                mask = df['clean_sel'].str.contains('anchor|turbo|top 5', na=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    t = str(row[ticker_col]).strip().upper()
                    watchlist[t] = str(row[sel_col])
                logs.append(f"✅ {prefix}: Found {len(filtered)} items")
            else:
                logs.append(f"⚠️ {prefix}: Selection column not found!")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# --- 2. ביצועי פורטפוליו (Day% מ-16:30 / Wk% מיום שני 17:00) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist is empty. Check CSV content.\n"
    
    # זמן ישראל (IDT - UTC+3 באפריל 2026)
    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_monday = now_isr.weekday()
    monday_date = (now_isr - datetime.timedelta(days=days_to_monday)).date()
    
    report = "📈 *WTC Portfolio Watch (Dynamic)*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t in watchlist.keys():
        try:
            # הורדת נתונים מפורטת (15 דקות לדיוק זמנים)
            data = yf.download(t, start=monday_date.strftime('%Y-%m-%d'), interval="15m", progress=False)
            if data.empty: continue
            
            curr_p = data['Close'].iloc[-1]
            
            # א. Day% - שינוי מהפתיחה של היום (16:30 ישראל = 13:30 UTC)
            today_data = data[data.index.date == now_isr.date()]
            day_open = today_data['Open'].iloc[0] if not today_data.empty else curr_p
            day_chg = ((curr_p / day_open) - 1) * 100
            
            # ב. Wk% - שינוי מיום שני ב-17:00 (17:00 ישראל = 14:00 UTC)
            monday_data = data[data.index.date == monday_date]
            # מחפשים את הנר של 14:00 UTC (17:00 ישראל)
            try:
                week_start_price = monday_data[monday_data.index.hour >= 14]['Open'].iloc[0]
            except:
                week_start_price = data['Open'].iloc[0] # גיבוי לפתיחת השבוע
            
            week_chg = ((curr_p / week_start_price) - 1) * 100
            
            # ג. Status - פריצת גבוה הבוקר (30 דקות ראשונות)
            opening_high = today_data.iloc[:2]['High'].max() if len(today_data) >= 2 else curr_p
            status = "✅ Brk" if curr_p >= opening_high else "❌ Bel"
            
            report += f"`{t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {week_chg:>+5.1f}% | {status}`\n"
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
        
        prompt = custom_prompt if custom_prompt else f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except Exception as e:
        return f"⚠️ שגיאת AI: {str(e)[:40]}"

# --- 4. סריקה וסיכום טכני ---
def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, pref)
        if df is not None:
            t_col = next((c for c in df.columns if 'ticker' in c.lower()), 'Ticker')
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
        send_telegram_msg(f"{header}🌙 *Closing Summary*\n\n{get_ai_report('סכם בעברית את יום המסחר.')}")

if __name__ == "__main__":
    main()
