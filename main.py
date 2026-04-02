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

# --- הגדרות תקשורת ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    # ניסיון שליחה עם Markdown, אם נכשל (בגלל תווים מיוחדים) שולח כטקסט פשוט
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
        # חיפוש הקובץ הכי חדש שמכיל את הקידומת (למשל GoldenPlanSTOCKS)
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
        files = res.get('files', [])
        
        if not files:
            # ניסיון נוסף ללא פילטר MimeType ליתר ביטחון
            res = service.files().list(q=f"name contains '{prefix}'", orderBy="createdTime desc").execute()
            files = res.get('files', [])
            if not files: return None, f"קובץ {prefix} לא נמצא"

        file_id = files[0]['id']
        file_name = files[0]['name']
        
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        fh.seek(0)
        df = pd.read_csv(fh)
        # נירמול עמודות (הופך ל-Ticker ו-Score)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, f"✅ נטען: {file_name}"
    except Exception as e:
        return None, f"❌ שגיאה: {str(e)}"

# --- דאשבורד נתוני שוק ---
def get_market_dashboard():
    try:
        spy = yf.Ticker("SPY").history(period="2d")
        vix = yf.Ticker("^VIX").history(period="1d")
        v_p = vix['Close'].iloc[-1]
        s_p = spy['Close'].iloc[-1]
        s_change = ((spy['Close'].iloc[-1] / spy['Close'].iloc[-2]) - 1) * 100
        
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        
        return (
            f"📊 *WTC Sentinel Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Market Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{v_p:.2f}`\n"
            f"📈 *SPY:* `{s_p:.2f} ({s_change:+.2f}%)`\n"
            f"`--------------------------`\n"
        )
    except: return "⚠️ Dashboard Unavailable\n\n"

# --- ניתוח AI מוסדי (Goldman Sachs Style) ---
def get_institutional_report():
    news_text = ""
    # איסוף חדשות על המדדים המרכזיים
    for t in ["^GSPC", "^IXIC", "^VIX", "GC=F", "CL=F"]:
        try:
            ticker_news = yf.Ticker(t).news
            for n in ticker_news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news_text += f"- {title}\n"
        except: continue
    
    prompt = f"""
    אתה אנליסט מוסדי בכיר בוול סטריט בסגנון Goldman Sachs.
    נתח את החדשות הבאות: {news_text}
    
    בנה דוח בעברית מקצועית בפורמט הבא:
    ## דוח ניתוח שוק - תמונת מצב אסטרטגית
    ### 🏛️ 1. 'הכסף הגדול': מוסדיים ואסטרטגיה
    (ניתוח עומק של הקצאת הון ופעילות מוסדיים)
    
    ### 💣 2. 'מוקשים ומאקרו': סיכונים וגיאופוליטיקה
    (ניתוח סיכוני ריבית, אינפלציה ואירועים גיאופוליטיים)
    
    ### 🌡️ 3. 'סנטימנט השוק': שורה תחתונה לסוחר
    (סיכום Risk-On/Off והמלצה פרקטית לסוחר התוך-יומי)
    """
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        models = client.models.list()
        target = next((m.name for m in models if 'flash' in m.name), 'gemini-1.5-flash')
        response = client.models.generate_content(model=target, contents=prompt)
        return response.text
    except Exception as e:
        return f"⚠️ שגיאת AI בניתוח המוסדי: {str(e)}"

# --- סריקת פריצות (Execution Engine) ---
def run_execution_scan(service):
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    # הגנת פתיחת מסחר
    if now.hour < 16 or (now.hour == 16 and now.minute < 30):
        return "🛑 *Market is Closed.* הסריקה תחל לאחר 16:30."
    
    results = {"Gold": [], "Underdogs": []}
    files_log = []
    
    # סריקה לפי השמות בדרייב שלך
    for prefix in ["GoldenPlanSTOCKS", "GoldenPlanETF"]:
        df, status = download_latest_file(service, prefix)
        files_log.append(status)
        
        if df is not None and 'Ticker' in df.columns:
            for _, row in df.iterrows():
                ticker = str(row['Ticker']).strip()
                score = row.get('Score', 0)
                try:
                    # בדיקת נתונים של 5 דקות
                    data = yf.download(ticker, period="1d", interval="5m", progress=False)
                    if len(data) < 7: continue # דורש לפחות 35 דקות מסחר
                    
                    # חישוב הגבוה של חצי השעה הראשונה
                    opening_high = data.iloc[:6]['High'].max()
                    current_price = data['Close'].iloc[-1]
                    
                    # בדיקת פריצה
                    if current_price > opening_high:
                        if score >= 75: results["Gold"].append(ticker)
                        elif score < 60: results["Underdogs"].append(ticker)
                except: continue

    # בניית הודעת הסיכום
    report = f"🎯 *WTC Execution Scan Result:*\n"
    report += "\n".join(files_log) + "\n\n"
    report += f"🥇 *Gold:* {', '.join(results['Gold']) if results['Gold'] else 'None'}\n"
    report += f"🐕 *Underdogs:* {', '.join(results['Underdogs']) if results['Underdogs'] else 'None'}\n\n"
    
    # הוספת משפט הסטטוס החכם (ההסבר שביקשת)
    if not results["Gold"] and not results["Underdogs"]:
        vix_val = yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1]
        if vix_val > 22:
            report += "💡 *סטטוס:* השוק בלחץ מכירות ותנודתיות גבוהה; המניות ברשימה נסחרות מתחת לגבוה היומי - מומלץ להמתין להרגעה ב-VIX."
        else:
            report += "💡 *סטטוס:* השוק בדשדוש או חוסר מומנטום; לא זוהו פריצות איכותיות מעל ה-Opening High ברשימות המעקב."
    else:
        report += f"🚀 *סטטוס:* זוהו פריצות מומנטום ב-{len(results['Gold']) + len(results['Underdogs'])} נכסים. השוק מאפשר עסקאות פריצה."
        
    return report

# --- פונקציה ראשית ---
def main():
    try:
        service = get_drive_service()
        is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
        
        db = get_market_dashboard()

        if is_manual:
            send_telegram_msg(f"{db}🛡️ *WTC Sentinel - Manual Mode*")
            send_telegram_msg(get_institutional_report())
            send_telegram_msg(run_execution_scan(service))
            return

        # הרצות אוטומטיות (לפי ה-Cron ב-YAML)
        if now.hour == 16:
            send_telegram_msg(f"{db}\n{get_institutional_report()}")
        elif now.hour >= 17 and now.hour < 22:
            send_telegram_msg(f"{db}\n{run_execution_scan(service)}")
            
    except Exception as e:
        send_telegram_msg(f"⚠️ שגיאה כללית בהרצת המערכת: {str(e)}")

if __name__ == "__main__":
    main()
    
