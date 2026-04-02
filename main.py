import os
import datetime
import time
import json
import pandas as pd
import yfinance as yf
import requests
from google import genai
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
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
    time.sleep(1.5)

# --- ניהול זיכרון (sentinel_state.json) ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def check_if_already_sent(service, slot_name):
    today = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)).strftime("%Y-%m-%d")
    query = "name = 'sentinel_state.json'"
    res = service.files().list(q=query, fields="files(id)").execute()
    files = res.get('files', [])
    if not files: return False, {}
    try:
        req = service.files().get_media(fileId=files[0]['id'])
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        state = json.loads(fh.getvalue().decode())
        if state.get(today) and slot_name in state[today]:
            return True, state
        return False, state
    except: return False, {}

def mark_as_sent(service, slot_name, state):
    today = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)).strftime("%Y-%m-%d")
    if today not in state: state[today] = []
    if slot_name not in state[today]: state[today].append(slot_name)
    fh = io.BytesIO(json.dumps(state).encode())
    media = MediaIoBaseUpload(fh, mimetype='application/json')
    query = "name = 'sentinel_state.json'"
    res = service.files().list(q=query, fields="files(id)").execute()
    if res.get('files'):
        service.files().update(fileId=res['files'][0]['id'], media_body=media).execute()
    else:
        service.files().create(body={'name': 'sentinel_state.json'}, media_body=media).execute()

# --- דאשבורד וסריקה ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p, s_p = vix['Close'].iloc[-1], spy['Close'].iloc[-1]
        s_c = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return f"📊 *WTC Dashboard*\n`--------------------------`\n🚦 *Status:* `{status}` {emoji}\n📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n`--------------------------`\n"
    except: return "⚠️ Dashboard Offline\n\n"

def download_latest_csv(service, prefix):
    try:
        # 1. מוצא את תיקיית WTC_SYSTEM
        res = service.files().list(q="name = 'WTC_SYSTEM' and mimeType = 'application/vnd.google-apps.folder'").execute()
        if not res.get('files'): return None
        f_id = res['files'][0]['id']
        # 2. מוצא את הקובץ הכי חדש עם הקידומת
        res = service.files().list(q=f"'{f_id}' in parents and name contains '{prefix}'", orderBy="createdTime desc").execute()
        if not res.get('files'): return None
        # 3. מוריד
        req = service.files().get_media(fileId=res['files'][0]['id'])
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    except: return None

def get_institutional_context():
    context = ""
    for t in ["^GSPC", "^IXIC", "^VIX", "GC=F", "CL=F"]:
        try:
            news = yf.Ticker(t).news
            for n in news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: context += f"- {title}\n"
        except: continue
    prompt = f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {context}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except Exception as e: return f"שגיאת AI: {e}"

def run_execution_scan(service):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    if now.hour < 16 or (now.hour == 16 and now.minute < 30): return "🛑 *Market Closed.* (Opens at 16:30)"
    if now.hour == 16 and now.minute < 65: return "⏳ *Waiting for Opening Range Data (17:05)...*"
    
    results = {"Gold": [], "Underdogs": []}
    for p in ["WTC_Intelligence_Stocks", "WTC_Intelligence_ETFs"]:
        df = download_latest_csv(service, p)
        if df is not None:
            for _, row in df.iterrows():
                ticker, score = row['Ticker'], row.get('Score', 0)
                try:
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue
                    if data['Close'].iloc[-1] > data.iloc[:6]['High'].max():
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue
    return f"🎯 *Execution Scan Result:*\n🥇 *Gold:* {', '.join(results['Gold']) or 'None'}\n🐕 *Underdogs:* {', '.join(results['Underdogs']) or 'None'}"

# --- MAIN ---
def main():
    service = get_drive_service()
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now.hour
    db = get_market_dashboard()

    if is_manual:
        send_telegram_msg(f"{db}🛡️ *Manual Sentinel Check*")
        send_telegram_msg(get_institutional_context())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        sent, state = check_if_already_sent(service, "16:00")
        if not sent:
            send_telegram_msg(f"{db}\n{get_institutional_context()}")
            mark_as_sent(service, "16:00", state)
    elif hour >= 17 and hour < 22: # טווח רחב כדי למנוע פספוס
        sent, state = check_if_already_sent(service, "17:00")
        if not sent:
            send_telegram_msg(f"{db}\n{run_execution_scan(service)}")
            mark_as_sent(service, "17:00", state)

if __name__ == "__main__":
    main()
