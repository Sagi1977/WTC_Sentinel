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

# --- חיבור והורדה מגוגל דרייב ---
def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_file(service, prefix):
    try:
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
        files = res.get('files', [])
        if not files: return None, f"קובץ {prefix} לא נמצא"

        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, "success"
    except Exception as e:
        return None, str(e)

# --- דאשבורד וניתוח AI ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p, s_p = vix['Close'].iloc[-1], spy['Close'].iloc[-1]
        s_c = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return f"📊 *WTC Sentinel Dashboard*\n`--------------------------`\n🚦 *Status:* `{status}` {emoji}\n📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n`--------------------------`\n"
    except: return "⚠️ Dashboard Offline\n\n"

def get_institutional_report():
    news_text = ""
    for t in ["^GSPC", "^VIX", "GC=F"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news_text += f"- {title}\n"
        except: continue
    
    prompt = f"""
    ענה בעברית כמחלקת מחקר של גולדמן סאקס. נתח חדשות: {news_text}
    בנה דוח בנקודות:
    ## דוח אסטרטגי - תמונת מצב מוסדית
    ### 🏛️ 1. 'הכסף הגדול': מוסדיים ואסטרטגיה
    ### 💣 2. 'מוקשים ומאקרו': סיכונים וגיאופוליטיקה
    ### 🌡️ 3. 'סנטימנט השוק': שורה תחתונה לסוחר
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח המוסדי."

# --- סריקת פריצות עם משפט הסבר ---
def run_execution_scan(service):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    if now.hour < 16 or (now.hour == 16 and now.minute < 30):
        return "🛑 *Market Closed.*"
    
    results = {"Gold": [], "Underdogs": []}
    files_found = []
    
    for prefix in ["GoldenPlanSTOCKS", "GoldenPlanETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            files_found.append(prefix)
            for _, row in df.iterrows():
                if 'Ticker' not in df.columns: continue
                ticker = str(row['Ticker']).strip()
                score = row.get('Score', 0)
                try:
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue
                    opening_high = data.iloc[:6]['High'].max()
                    current_price = data['Close'].iloc[-1]
                    if current_price > opening_high:
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue

    # בניית משפט הסטטוס (ההסבר שביקשת)
    if not results["Gold"] and not results["Underdogs"]:
        vix = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
        if vix > 22:
            status_summary = "💡 *סיכום סריקה:* השוק בלחץ מכירות ותנודתיות גבוהה; אף נכס לא הצליח להחזיק מעמד מעל גבוה הבוקר - מומלץ להמתין להרגעה."
        else:
            status_summary = "💡 *סיכום סריקה:* השוק בדשדוש; לא זוהו פריצות מומנטום מעל ה-Opening High ברשימות המעקב."
    else:
        status_summary = f"🚀 *סיכום סריקה:* זוהו פריצות מומנטום ב-{len(results['Gold']) + len(results['Underdogs'])} נכסים."

    report = f"🎯 *WTC Execution Scan*\n"
    report += f"📂 קבצים שנסרקו: {', '.join(files_found) if files_found else 'אף קובץ'}\n\n"
    report += f"🥇 *Gold:* {', '.join(results['Gold']) if results['Gold'] else 'None'}\n"
    report += f"🐕 *Underdogs:* {', '.join(results['Underdogs']) if results['Underdogs'] else 'None'}\n\n"
    report += status_summary
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    db = get_market_dashboard()

    if is_manual:
        send_telegram_msg(f"{db}🛡️ *Manual Mode Run*")
        send_telegram_msg(get_institutional_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if now.hour == 16:
        send_telegram_msg(f"{db}\n{get_institutional_report()}")
    elif now.hour >= 17 and now.hour < 22:
        send_telegram_msg(f"{db}\n{run_execution_scan(service)}")

if __name__ == "__main__":
    main()
