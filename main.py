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

# --- הגדרות סודות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

# --- פונקציית ה-AI ---
def get_ai_analysis(prompt):
    model = genai.GenerativeModel('gemini-1.5-flash')
    response = model.generate_content(prompt)
    return response.text

# --- משיכת חדשות מוסדיים ---
def get_market_context():
    # מושך חדשות על השוק ועל דמויות מפתח
    search_query = "Tom Lee Fundstrat, Goldman Sachs market outlook, Fed interest rate news"
    tickers = ["^GSPC", "^IXIC", "VIX"]
    context_data = ""
    
    for t in tickers:
        ticker_data = yf.Ticker(t)
        news = ticker_data.news[:3]
        for n in news:
            context_data += f"- {n['title']}\n"
    
    prompt = f"""
    אתה אנליסט מוסדי בכיר בוול סטריט. נתח את כותרות החדשות הבאות:
    {context_data}
    
    בנה דו"ח קצר לטלגרם הכולל:
    1. 'הכסף הגדול': מה המוסדיים (גולדמן, טום לי) חושבים היום?
    2. 'מוקשים': האם יש הודעת פד או אירוע מאקרו קריטי?
    3. 'סנטימנט': האם השוק במצב של Risk-On או Risk-Off?
    תכתוב בעברית קולחת ומקצועית.
    """
    return get_ai_analysis(prompt)

# --- לוגיקת סריקה (דו"ח 17:00) ---
def run_execution_scan():
    # (הקוד הקיים שלך מהשלב הקודם שסורק את הדרייב ומחפש פריצות)
    return "נציג כאן את תוצאות הפריצות שמצאנו בדרייב..."

# --- דו"ח נעילה (23:00) ---
def get_closing_summary():
    prompt = "סכם את יום המסחר בוול סטריט בעברית. מי הסקטורים שניצחו ומי הפסידו? מה התובנה המרכזית למחר?"
    return get_ai_analysis(prompt)

# --- המוח המרכזי ---
def main():
    now_israel = datetime.datetime.now() + datetime.timedelta(hours=2) # התאמה לשעון ישראל
    hour = now_israel.hour
    
    if hour == 16:
        print("Running Context Report...")
        report = f"🏛️ *WTC Institutional Intelligence (16:00)*\n\n{get_market_context()}"
        send_telegram_msg(report)
    
    elif hour == 17:
        print("Running Execution Report...")
        # כאן תרוץ פונקציית הסריקה הקודמת שלך
        # ...
        send_telegram_msg("🎯 *WTC 17:00 Execution Report*...")
        
    elif hour == 23:
        print("Running Closing Report...")
        report = f"🌙 *WTC Daily Closing Summary*\n\n{get_closing_summary()}"
        send_telegram_msg(report)

if __name__ == "__main__":
    main()
