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

# --- גוגל דרייב - טעינה חסינה ---
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
        # utf-8-sig מטפל ב-BOM ובאמוג'ים ב-CSV
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python')
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

# --- 1. בניית רשימת מעקב (זיהוי אגרסיבי של עמודות) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            # ניקוי שמות עמודות מכל מה שאינו אותיות/מספרים
            clean_cols = {c: re.sub(r'[^a-zA-Z0-9]', '', str(c)).lower() for c in df.columns}
            df = df.rename(columns=clean_cols)
            
            sel_col = next((c for c in df.columns if 'final' in c or 'selection' in c), None)
            ticker_col = next((c for c in df.columns if 'ticker' in c), 'ticker')
            
            if sel_col:
                mask = df[sel_col].astype(str).str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    ticker = str(row[ticker_col]).strip().upper()
                    watchlist[ticker] = str(row[sel_col])
                logs.append(f"✅ {prefix}: מצאתי {len(filtered)} נכסים")
            else:
                logs.append(f"⚠️ {prefix}: עמודת Selection לא זוהתה. עמודות: {list(df.columns)[:4]}")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# --- 2. ביצועי פורטפוליו (Day% מ-16:30 / Wk% מיום שני 17:00) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ רשימת המעקב ריקה - בדוק את התוכן בדרייב.\n"
    
    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_monday = now_isr.weekday()
    monday_date = (now_isr - datetime.timedelta(days=days_to_monday)).date()
    
    report = "📈 *WTC Portfolio Watch (Anchor & Turbo)*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t in watchlist.keys():
        try:
            # הורדת נתונים מיום שני בנרות של 15 דקות
            data = yf.download(t, start=monday_date.strftime('%Y-%m-%d'), interval="15m", progress=False)
            if data.empty: continue
            
            curr_p = data['Close'].iloc[-1]
            
            # Day% - שינוי מפתיחת היום (16:30 ישראל = 13:30 UTC)
            today_data = data[data.index.date == now_isr.date()]
            day_open = today_data['Open'].iloc[0] if not today_data.empty else curr_p
            day_chg = ((curr_p / day_open) - 1) * 100
            
            # Wk% - שינוי מיום שני ב-17:00 (17:00 ישראל = 14:00 UTC)
            monday_data = data[data.index.date == monday_date]
            # לוקח את הנר הראשון ביום שני שמתחיל ב-14:00 (17:00 ישראל) או אחריו
            try:
                purchase_price = monday_data[monday_data.index.hour >= 14]['Open'].iloc[0]
            except:
                purchase_price = data['Open'].iloc[0]
            
            wk_chg = ((curr_p / purchase_price) - 1) * 100
            
            # Status - פריצת גבוה הבוקר (30 דקות ראשונות = 2 נרות של 15 דק')
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
        
        p = custom_prompt if custom_prompt else f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח חדשות: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except Exception as e:
        return f"⚠️ שגיאת AI: {str(e)[:40]}"

# --- 4. סריקה וסיכום טכני ---
def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, prefix=pref)
        if df is not None:
            t_col = next((c for c in df.columns if 'ticker' in str(c).lower()), 'ticker')
            for _, row in df.iterrows():
                t = str(row.get(t_col, '')).strip()
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) >= 7 and d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        res[label].append(t)
                except: continue

    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    report = f"🎯 *WTC Execution Scan Result:*\n🥇 STOCKS Gold: {', '.join(res['STOCKS']) or 'None'}\n🏅 ETF Gold: {', '.join(res['ETF']) or 'None'}\n\n"
    
    if not res["STOCKS"] and not res["ETF"]:
        report += "💡 *סיכום טכני:* אין פריצות מעל גבוה הבוקר. " + ("מומלץ להמתין להרגעה ב-VIX." if vix > 22 else "השוק בדשדוש.")
    else:
        report += f"🚀 *סיכום טכני:* זוהו פריצות מומנטום ב-{len(res['STOCKS']) + len(res['ETF'])} נכסים."
    return report

# --- MAIN - ניהול תזמון ---
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

    # 1. הרצה ידנית: שולח הכל
    if is_manual:
        send_telegram_msg(f"{header}\n{perf}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    # 2. עדכון 16:00 (פתיחה): דוח אסטרטגי ו-AI
    if hour == 16:
        send_telegram_msg(f"{header}\n{get_ai_report()}")
    
    # 3. עדכון 17:00 (זמן מסחר): פורטפוליו וסריקת פריצות
    elif 17 <= hour < 23:
        send_telegram_msg(f"{header}\n{perf}\n{run_execution_scan(service)}")
    
    # 4. עדכון 23:00 (סגירה): סיכום AI ונעילת יום
    elif hour == 23:
        send_telegram_msg(f"{header}🌙 *WTC Closing Summary*\n\n{get_ai_report('סכם בעברית את יום המסחר ואיך השוק נסגר.')}")

if __name__ == "__main__":
    main()
