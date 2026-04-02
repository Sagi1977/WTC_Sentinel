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
    # שליחה בטוחה: אם המארקדאון נכשל, שולח כטקסט פשוט
    res = requests.post(url, json={"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "Markdown"})
    if res.status_code != 200:
        requests.post(url, json={"chat_id": CHAT_ID, "text": text[:4000]})
    time.sleep(1)

# --- אבחון זהות וגישה ---
def get_diagnostics(service):
    try:
        # 1. מי אני?
        creds, _ = google.auth.default()
        email = getattr(creds, 'service_account_email', "Unknown")
        
        # 2. מה אני רואה?
        results = service.files().list(pageSize=10, fields="files(name)").execute()
        files = [f['name'] for f in results.get('files', [])]
        
        diag = f"🔑 *Bot Identity:* `{email}`\n"
        diag += f"📂 *Visible items:* {', '.join(files) if files else 'None (Access Denied)'}"
        return diag, email
    except Exception as e:
        return f"❌ Diagnostic Error: {str(e)}", "Error"

# --- סריקת מניות (הלוגיקה המלאה) ---
def run_full_scan(service):
    results = {"Gold": [], "Underdogs": []}
    log = ""
    
    # איתור תיקיית WTC_SYSTEM
    folders = service.files().list(q="name = 'WTC_SYSTEM' and mimeType = 'application/vnd.google-apps.folder'").execute().get('files', [])
    if not folders:
        return "❌ תיקייה WTC_SYSTEM לא נמצאה בדרייב של הבוט."
    
    f_id = folders[0]['id']
    for prefix in ["WTC_Intelligence_Stocks", "WTC_Intelligence_ETFs"]:
        f_res = service.files().list(q=f"'{f_id}' in parents and name contains '{prefix}'", orderBy="createdTime desc").execute().get('files', [])
        if f_res:
            file_id = f_res[0]['id']
            req = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            MediaIoBaseDownload(fh, req).next_chunk()
            fh.seek(0)
            df = pd.read_csv(fh)
            df.columns = [c.strip().capitalize() for c in df.columns]
            
            log += f"📄 נסרקו {len(df)} מניות מ-{prefix.split('_')[-1]}\n"
            
            for _, row in df.iterrows():
                ticker = str(row['Ticker']).strip()
                score = row.get('Score', 0)
                try:
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue
                    if data['Close'].iloc[-1] > data.iloc[:6]['High'].max():
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue
        else:
            log += f"❓ קובץ {prefix} לא נמצא בתיקייה.\n"

    return f"🎯 *Scan Report:*\n{log}\n🥇 *Gold:* {', '.join(results['Gold']) or 'None'}\n🐕 *Underdogs:* {', '.join(results['Underdogs']) or 'None'}"

# --- דאשבורד ו-AI ---
def get_institutional_report():
    news = ""
    for t in ["^GSPC", "^VIX", "GC=F"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    
    prompt = f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח החדשות."

def main():
    creds, _ = google.auth.default()
    service = build('drive', 'v3', credentials=creds)
    
    # בדיקת דאשבורד (VIX/SPY)
    spy = yf.Ticker("SPY").history(period="1d")['Close'].iloc[-1]
    vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
    dashboard = f"📊 *WTC Quick Look*\n`VIX: {vix:.2f} | SPY: {spy:.2f}`\n`--------------------------`\n"

    # הרצה
    diag_msg, bot_email = get_diagnostics(service)
    send_telegram_msg(f"{dashboard}{diag_msg}")
    
    send_telegram_msg(get_institutional_report())
    
    send_telegram_msg(run_full_scan(service))

if __name__ == "__main__":
    main()
