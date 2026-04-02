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
    
    # אם הטקסט ארוך מאוד (מעל 4000 תווים), טלגרם חוסמת. אנחנו נחתוך ליתר ביטחון.
    safe_text = text[:4000]
    
    payload = {"chat_id": CHAT_ID, "text": safe_text, "parse_mode": "Markdown"}
    res = requests.post(url, json=payload)
    
    if res.status_code != 200:
        # ניסיון שני ללא Markdown (מונע קריסה בגלל תווים מיוחדים)
        requests.post(url, json={"chat_id": CHAT_ID, "text": safe_text})
    time.sleep(1.5)

# --- ניהול מצב (State) מול גוגל דרייב ---
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
    files = res.get('files', [])
    
    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        service.files().create(body={'name': 'sentinel_state.json'}, media_body=media).execute()

# --- דאשבורד ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p = vix['Close'].iloc[-1]
        s_p = spy['Close'].iloc[-1]
        s_c = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return (
            f"📊 *WTC Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n"
            f"`--------------------------`\n"
        )
    except: return "⚠️ Dashboard Error\n\n"

# --- ניתוח AI ---
def get_institutional_context():
    context_data = ""
    for t in ["^GSPC", "^IXIC", "^VIX", "GC=F", "CL=F"]:
        try:
            news = yf.Ticker(t).news
            if news:
                for n in news[:2]:
                    title = n.get('title') or n.get('content', {}).get('title')
                    if title: context_data += f"- {title}\n"
        except: continue

    prompt = f"""
    ענה בעברית כגולדמן סאקס. נתח חדשות: {context_data}
    מבנה:
    ## דוח ניתוח שוק - WTC 2026
    ### 🏛️ 1. 'הכסף הגדול': מוסדיים
    ### 💣 2. 'מוקשים ומאקרו': סיכונים
    ### 🌡️ 3. 'סנטימנט השוק': שורה תחתונה
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        models = client.models.list()
        target = next((m.name for m in models if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except Exception as e: return f"שגיאת AI: {e}"

# --- סריקת מניות מהדרייב (הלוגיקה המלאה ששוחזרה) ---
def download_latest_csv(service, prefix):
    try:
        query = f"name = 'WTC_SYSTEM' and mimeType = 'application/vnd.google-apps.folder'"
        res = service.files().list(q=query).execute()
        if not res.get('files'): return None
        folder_id = res['files'][0]['id']
        
        query = f"'{folder_id}' in parents and name contains '{prefix}'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        if not res.get('files'): return None
        
        req = service.files().get_media(fileId=res['files'][0]['id'])
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    except: return None

def run_execution_scan(service):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    
    # הגנות זמן
    if now.hour < 16 or (now.hour == 16 and now.minute < 30):
        return "🛑 *Market Closed.* (Opens at 16:30)"
    if now.hour == 16 and now.minute < 65:
        return "⏳ *Waiting for Opening Range data (17:05)...*"

    results = {"Gold": [], "Underdogs": []}
    for prefix in ["WTC_Intelligence_Stocks", "WTC_Intelligence_ETFs"]:
        df = download_latest_csv(service, prefix)
        if df is not None:
            for _, row in df.iterrows():
                ticker = row['Ticker']
                score = row.get('Score', 0)
                try:
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue
                    # בדיקת פריצה מעל הגבוה של חצי השעה הראשונה
                    opening_high = data.iloc[:6]['High'].max()
                    current_price = data['Close'].iloc[-1]
                    if current_price > opening_high:
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue
    
    return (
        f"🎯 *Execution Scan Result:*\n"
        f"🥇 *Gold:* {', '.join(results['Gold']) if results['Gold'] else 'None'}\n"
        f"🐕 *Underdogs:* {', '.join(results['Underdogs']) if results['Underdogs'] else 'None'}"
    )

# --- פונקציה ראשית ---
def main():
    service = get_drive_service()
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now.hour
    
    db = get_market_dashboard()

    if is_manual:
        print("Starting Manual Run...")
        send_telegram_msg(f"{db}🛡️ *Sentinel Manual Check*")
        send_telegram_msg(get_institutional_context())
        send_telegram_msg(run_execution_scan(service))
        return

    # הרצה אוטומטית (עם מנגנון זיכרון)
    if hour == 16:
        sent, state = check_if_already_sent(service, "16:00")
        if not sent:
            send_telegram_msg(f"{db}\n{get_institutional_context()}")
            mark_as_sent(service, "16:00", state)
            
    elif hour == 17:
        sent, state = check_if_already_sent(service, "17:00")
        if not sent:
            report = run_execution_scan(service)
            send_telegram_msg(f"{db}\n{report}")
            mark_as_sent(service, "17:00", state)

if __name__ == "__main__":
    main()
