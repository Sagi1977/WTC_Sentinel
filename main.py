import os
import datetime
import pandas as pd
import yfinance as yf
import requests
from google import genai
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
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
    requests.post(url, json=payload)

# --- פונקציית הנראות (Dashboard) ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        
        spy_price = spy['Close'].iloc[-1]
        spy_change = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        vix_price = vix['Close'].iloc[-1]
        
        status = "BULLISH" if vix_price < 18 else "CAUTION" if vix_price < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        
        # המלצת פעולה מהירה
        action = "Market is healthy." if status == "BULLISH" else "Trade with smaller sizes." if status == "CAUTION" else "High risk! Protect capital."
        
        dashboard = (
            f"📊 *WTC Intelligence Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{vix_price:.2f}`\n"
            f"📈 *SPY:* `{spy_price:.2f} ({spy_change:+.2f}%)`\n"
            f"`--------------------------`\n"
            f"💡 *Action:* `{action}`\n\n"
        )
        return dashboard
    except:
        return "⚠️ Dashboard Data Unavailable\n\n"

# --- מנגנון AI דינמי ---
def get_ai_response(prompt):
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        models_list = client.models.list()
        target_model = next((m.name for m in models_list if 'flash' in m.name), 'gemini-1.5-flash')
        response = client.models.generate_content(model=target_model, contents=prompt)
        return response.text
    except Exception as e:
        return f"שגיאת AI: {str(e)}"

# --- גוגל דרייב ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_csv(service, folder_name, file_prefix):
    try:
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
        res = service.files().list(q=query, fields="files(id)").execute()
        if not res.get('files'): return None
        f_id = res['files'][0]['id']
        query = f"'{f_id}' in parents and name contains '{file_prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
        if not res.get('files'): return None
        req = service.files().get_media(fileId=res['files'][0]['id'])
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh)
    except: return None

# --- דו"חות ---
def get_institutional_context():
    context_data = ""
    for t in ["^GSPC", "^IXIC", "VIX"]:
        try:
            news = yf.Ticker(t).news
            if news:
                for n in news[:2]:
                    title = n.get('title') or n.get('content', {}).get('title')
                    if title: context_data += f"- {title}\n"
        except: continue
    return get_ai_response(f"נתח את הסנטימנט בוול סטריט בעברית: {context_data if context_data else 'אין חדשות'}")

def run_execution_scan():
    try:
        service = get_drive_service()
        results = {"Gold": [], "Underdogs": []}
        for prefix in ["WTC_Intelligence_Stocks", "WTC_Intelligence_ETFs"]:
            df = download_latest_csv(service, "WTC_SYSTEM", prefix)
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
        return f"🥇 *Gold:* {', '.join(results['Gold']) or 'None'}\n🐕 *Underdogs:* {', '.join(results['Underdogs']) or 'None'}"
    except Exception as e: return f"שגיאת סריקה: {e}"

# --- המוח המרכזי ---
def main():
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    
    # יצירת הדאשבורד המשותף לכל ההודעות
    dashboard = get_market_dashboard()

    if is_manual:
        send_telegram_msg(f"{dashboard}🛡️ *WTC Sentinel 2026 - Manual Mode*")
        send_telegram_msg(f"🏛️ *Institutional Context:*\n{get_institutional_context()}")
        send_telegram_msg(f"🎯 *Market Scan:*\n{run_execution_scan()}")
        return

    if now.hour == 16:
        send_telegram_msg(f"{dashboard}🏛️ *Institutional Intelligence*\n\n{get_institutional_context()}")
    elif now.hour == 17:
        send_telegram_msg(f"{dashboard}🎯 *WTC Execution Report*\n\n{run_execution_scan()}")
    elif now.hour == 23:
        summary = get_ai_response("סכם את יום המסחר בעברית.")
        send_telegram_msg(f"{dashboard}🌙 *WTC Closing Summary*\n\n{summary}")

if __name__ == "__main__":
    main()
