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

# --- הגדרות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    res = requests.post(url, json=payload)
    if res.status_code != 200:
        requests.post(url, json={"chat_id": CHAT_ID, "text": text})
    time.sleep(1)

# --- מנגנון זיכרון (State Management) ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def check_if_already_sent(service, slot_name):
    """בודק בדרייב האם הדו"ח לשעה זו כבר נשלח היום"""
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    state_file_name = "sentinel_state.json"
    
    try:
        # חיפוש הקובץ בדרייב
        query = f"name = '{state_file_name}'"
        res = service.files().list(q=query, fields="files(id)").execute()
        files = res.get('files', [])
        
        if files:
            file_id = files[0]['id']
            req = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            MediaIoBaseDownload(fh, req).next_chunk()
            state = json.loads(fh.getvalue().decode())
        else:
            state = {}

        if state.get(today) and slot_name in state[today]:
            return True, state # כבר נשלח
        return False, state # טרם נשלח
    except:
        return False, {}

def mark_as_sent(service, slot_name, state):
    """מעדכן בדרייב שהדו"ח נשלח"""
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    if today not in state: state[today] = []
    state[today].append(slot_name)
    
    state_file_name = "sentinel_state.json"
    fh = io.BytesIO(json.dumps(state).encode())
    media = MediaIoBaseUpload(fh, mimetype='application/json')
    
    query = f"name = '{state_file_name}'"
    res = service.files().list(q=query, fields="files(id)").execute()
    files = res.get('files', [])
    
    if files:
        service.files().update(fileId=files[0]['id'], media_body=media).execute()
    else:
        file_metadata = {'name': state_file_name}
        service.files().create(body=file_metadata, media_body=media, fields='id').execute()

# --- דאשבורד ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_price = vix['Close'].iloc[-1]
        s_price = spy['Close'].iloc[-1]
        s_change = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        status = "BULLISH" if v_price < 18 else "CAUTION" if v_price < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return (
            f"📊 *WTC Intelligence Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{v_price:.2f}` | 📈 *SPY:* `{s_price:.2f} ({s_change:+.2f}%)`\n"
            f"`--------------------------`\n"
        )
    except: return "⚠️ Dashboard Offline\n\n"

# --- דו"ח AI מפורט ---
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
    ענה בעברית מקצועית כגולדמן סאקס. נתח: {context_data}
    מבנה הדו"ח:
    ## דוח ניתוח שוק - תמונת מצב אסטרטגית ל-2026
    ### 🏛️ 1. 'הכסף הגדול': מוסדיים ואסטרטגיה
    ### 💣 2. 'מוקשים ומאקרו': סיכונים וגיאופוליטיקה
    ### 🌡️ 3. 'סנטימנט השוק': שורה תחתונה לסוחר
    """
    client = genai.Client(api_key=GEMINI_KEY)
    models = client.models.list()
    target = next((m.name for m in models if 'flash' in m.name), 'gemini-1.5-flash')
    return client.models.generate_content(model=target, contents=prompt).text

def run_execution_scan():
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    if now.hour == 16 and now.minute < 30: return "🛑 *Market Closed.* Opens at 16:30."
    if now.hour == 16 and now.minute < 65: return "⏳ *Waiting for Opening Range (17:05)...*"
    return "🎯 *Execution Scan:* (סריקת מניות מהדרייב תופיע כאן ב-17:05)"

# --- המוח המרכזי ---
def main():
    service = get_drive_service()
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now.hour
    
    db = get_market_dashboard()

    if is_manual:
        send_telegram_msg(f"{db}🛡️ *Manual Health Check*")
        send_telegram_msg(get_institutional_context())
        send_telegram_msg(run_execution_scan())
        return

    # ניהול חלונות זמן
    if hour == 16:
        sent, state = check_if_already_sent(service, "16:00")
        if not sent:
            send_telegram_msg(f"{db}\n{get_institutional_context()}")
            mark_as_sent(service, "16:00", state)
            
    elif hour == 17:
        sent, state = check_if_already_sent(service, "17:00")
        if not sent:
            send_telegram_msg(f"{db}\n🎯 *WTC Execution*\n{run_execution_scan()}")
            mark_as_sent(service, "17:00", state)

if __name__ == "__main__":
    main()
