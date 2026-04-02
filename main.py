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

# --- גוגל דרייב - טעינה אגרסיבית ---
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
        # שימוש בקידוד הכי רחב שיש כדי שלא יתעלם מהאמוג'ים שלך
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python', on_bad_lines='skip')
        df.columns = [str(c).strip() for c in df.columns]
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

# --- 1. בניית רשימה (חיפוש עיוור בתוך הקובץ) ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            # מוצא את עמודת ה-Ticker ועמודת ה-Selection לא משנה איך קראת להן
            t_col = next((c for c in df.columns if 'Ticker' in c), 'Ticker')
            s_col = next((c for c in df.columns if 'Selection' in c or 'Final' in c), None)
            
            if s_col:
                # הוא מחפש את המילים בתוך התא, לא משנה מה יש מסביב
                mask = df[s_col].astype(str).str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    ticker = str(row[t_col]).strip().upper()
                    watchlist[ticker] = row[s_col]
                logs.append(f"✅ {prefix}: מצאתי {len(filtered)} נכסים")
            else:
                logs.append(f"⚠️ {prefix}: לא נמצאה עמודת בחירה!")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# --- 2. חישוב ביצועים (הדיוק שביקשת) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ הטבלה ריקה - וודא שהמילים Anchor/Turbo מופיעות ב-CSV.\n"
    
    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_monday = now_isr.weekday() # 0=Mon, 3=Thu
    monday_date = (now_isr - datetime.timedelta(days=days_to_monday)).date()
    
    report = "📈 *WTC Portfolio Performance*\n"
    report += "`Ticker | Price | Day% | Wk%  | Status`\n"
    report += "`---------------------------------------`\n"
    
    for t in watchlist.keys():
        try:
            # מביא נתונים מתחילת השבוע בנרות של שעה (יותר יציב לחישוב שבועי)
            data = yf.download(t, start=monday_date.strftime('%Y-%m-%d'), interval="1h", progress=False)
            if data.empty: continue
            
            curr_p = data['Close'].iloc[-1]
            
            # Wk% - מהפתיחה הראשונה של השבוע (יום שני)
            week_open = data['Open'].iloc[0]
            wk_chg = ((curr_p / week_open) - 1) * 100
            
            # Day% - מהפתיחה של היום (16:30)
            today_data = data[data.index.date == now_isr.date()]
            day_open = today_data['Open'].iloc[0] if not today_data.empty else week_open
            day_chg = ((curr_p / day_open) - 1) * 100
            
            # Status - בדיקת פריצה יומית
            # מוריד נתונים מהירים רק לצורך הסטטוס
            d5 = yf.download(t, period="1d", interval="5m", progress=False)
            o_high = d5.iloc[:6]['High'].max() if len(d5) >= 6 else curr_p
            status = "✅ Brk" if curr_p >= o_high else "❌ Bel"
            
            report += f"`{t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
        except: continue
        
    return report + "`---------------------------------------`\n"

# --- 3. AI וסיכום טכני ---
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

def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, pref)
        if df is not None:
            t_col = next((c for c in df.columns if 'Ticker' in c), 'Ticker')
            for _, row in df.iterrows():
                t = str(row.get(t_col, '')).strip()
                try:
                    d = yf.download(t, period="1d", interval="5m", progress=False)
                    if len(d) >= 7 and d['Close'].iloc[-1] > d.iloc[:6]['High'].max():
                        res[label].append(t)
                except: continue
    
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    report = f"🎯 *Scan Result:*\n🥇 STOCKS: {', '.join(res['STOCKS']) or 'None'}\n🏅 ETF: {', '.join(res['ETF']) or 'None'}\n\n"
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
