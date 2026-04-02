import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import google.generativeai as genai
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.auth
import io

# --- הגדרות וחיבורים ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

# --- פונקציות AI (Gemini) ---
def get_ai_analysis(prompt):
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"שגיאת AI: {str(e)}"

# --- דו"ח 16:00 - הקשר מוסדי ומאקרו ---
def get_market_context():
    tickers = ["^GSPC", "^IXIC", "VIX", "GC=F"] # מדדים, ויקס וזהב
    context_data = ""
    for t in tickers:
        ticker_news = yf.Ticker(t).news[:2]
        for n in ticker_news:
            context_data += f"- {n['title']}\n"
    
    prompt = f"""
    אתה אנליסט מוסדי בכיר. נתח את החדשות הבאות:
    {context_data}
    
    בנה דו"ח קצר לטלגרם (בעברית):
    1. מה הכסף הגדול (המוסדיים) מתכנן היום?
    2. האם יש אירועי מאקרו/פד קריטיים?
    3. מה הסנטימנט הכללי (Risk-On/Off)?
    """
    return get_ai_analysis(prompt)

# --- דו"ח 17:00 - סריקת פריצות (Drive + Yahoo Finance) ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_csv(service, folder_name, file_prefix):
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    if not items: return None
    folder_id = items[0]['id']
    query = f"'{folder_id}' in parents and name contains '{file_prefix}' and mimeType = 'text/csv'"
    results = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files: return None
    request = service.files().get_media(fileId=files[0]['id'])
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done: _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

def run_execution_scan():
    service = get_drive_service()
    df_stocks = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_Stocks")
    df_etfs = download_latest_csv(service, "WTC_SYSTEM", "WTC_Intelligence_ETFs")
    
    found = []
    for df in [df_stocks, df_etfs]:
        if df is None: continue
        for _, row in df.iterrows():
            ticker = row['Ticker']
            try:
                data = yf.download(ticker, period="1d", interval="5m", progress=False)
                if len(data) < 7: continue
                if data['Close'].iloc[-1] > data.iloc[:6]['High'].max():
                    found.append(ticker)
            except: continue
    return ", ".join(found) if found else "None"

# --- דו"ח 23:00 - סיכום נעילה ---
def get_closing_summary():
    prompt = "סכם את יום המסחר בוול סטריט בעברית. מי ניצח היום ומה התובנה הכי חשובה למחר?"
    return get_ai_analysis(prompt)

# --- המוח המרכזי: ניהול זמנים ומצב ידני ---
def main():
    # זיהוי אם ההרצה היא ידנית דרך כפתור ה-Run Workflow
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    # חישוב זמן בישראל (UTC+3)
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour = now.hour

    if is_manual:
        # שים לב: כאן החלפתי למירכאות בודדות בחוץ כדי שהגרשיים של ה-דו"ח לא יפריעו
        send_telegram_msg('🧪 *הרצה ידנית מזוהה - מפיק דו"ח משולב לבדיקה...*')
        ctx = get_market_context()
        send_telegram_msg(f"🏛️ *Context Check:*\n{ctx}")
        return

    # הרצה אוטומטית לפי שעה (שעות שרת מותאמות לישראל)
    if hour == 16:
        send_telegram_msg(f"🏛️ *WTC Intelligence (16:00)*\n\n{get_market_context()}")
    elif hour == 17:
        res = run_execution_scan()
        send_telegram_msg(f"🎯 *WTC Execution (17:00)*\n\nBreakouts found: {res}")
    elif hour == 23:
        send_telegram_msg(f"🌙 *WTC Closing Summary*\n\n{get_closing_summary()}")
