import os
import datetime
import requests
from google import genai  # הספרייה החדשה של 2026

# --- הגדרות ---
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload)
        print(f"Telegram response: {r.status_code}")
    except Exception as e:
        print(f"Error sending to Telegram: {e}")

def get_ai_summary():
    try:
        # בגרסת 2026, ה-Client של google-genai יודע לנהל את הגרסאות לבד
        client = genai.Client(api_key=GEMINI_KEY)
        
        # שימוש במודל 1.5-flash שהוא הכי מהיר ויציב לשימוש חופשי
        response = client.models.generate_content(
            model='gemini-1.5-flash', 
            contents='תן משפט מוטיבציה קצר לסוחר מניות בעברית'
        )
        
        # בגרסה החדשה, הטקסט נמצא תחת response.text
        return response.text
    except Exception as e:
        # אם יש שגיאה, נדפיס אותה ללוג כדי שנבין מה קרה
        print(f"AI Error detail: {str(e)}")
        return f"שגיאת AI: {str(e)}"

def main():
    print("🚀 Script started!")
    
    # בדיקה האם ההרצה ידנית
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    if is_manual:
        print("Manual trigger detected!")
        msg = "👋 *הודעת בדיקה: המערכת מחוברת!*"
        msg += f"\n\n🤖 *בדיקת AI:* {get_ai_summary()}"
        send_telegram_msg(msg)
    else:
        # כאן תבוא הלוגיקה של השעות (16:00, 17:00 וכו')
        print("Scheduled run detected - checking hours...")
        # ... (הקוד הקודם שלך)

if __name__ == "__main__":
    main()
