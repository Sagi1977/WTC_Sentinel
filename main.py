import os
import datetime
import requests
import google.generativeai as genai # שים לב: זה ה-Import הנכון!

# --- הגדרות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

# הגדרת ה-AI
genai.configure(api_key=GEMINI_KEY)

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

def get_ai_summary():
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        response = model.generate_content('תן משפט מוטיבציה קצר לסוחר מניות בעברית')
        return response.text
    except Exception as e:
        return f"שגיאת AI: {str(e)}"

def main():
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    if is_manual:
        print("Manual run detected")
        # בדיקת קשר מיידית
        send_telegram_msg("✅ *המערכת מחוברת!* בודק AI כעת...")
        
        # שליחת תשובת ה-AI
        res = get_ai_summary()
        send_telegram_msg(f"🤖 *בינה מלאכותית אומרת:* \n{res}")
    else:
        # כאן תהיה הלוגיקה של השעות (16:00 וכו')
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
        print(f"Current hour: {now.hour}")

if __name__ == "__main__":
    main()
