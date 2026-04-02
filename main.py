import os
import datetime
import requests
import google.generativeai as genai

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
        # שלב הגילוי: בודק אילו מודלים באמת קיימים בחשבון שלך
        available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        print(f"DEBUG: Found models: {available_models}")
        
        if not available_models:
            return "לא נמצאו מודלים זמינים בחשבון ה-API שלך. וודא שהמפתח תקין."
            
        # בוחר את המודל הכי מתאים (מחפש flash, ואם אין - לוקח את הראשון ברשימה)
        selected_model = next((m for m in available_models if 'flash' in m), available_models[0])
        print(f"DEBUG: Using model: {selected_model}")
        
        model = genai.GenerativeModel(selected_model)
        response = model.generate_content('תן משפט מוטיבציה קצר לסוחר מניות בעברית')
        return response.text
    except Exception as e:
        return f"שגיאת AI מפורטת: {str(e)}"

def main():
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'
    
    if is_manual:
        print("Manual run started")
        send_telegram_msg("🛰️ *המערכת מבצעת סריקת מודלים...*")
        
        res = get_ai_summary()
        send_telegram_msg(f"🤖 *הודעה מה-AI:* \n{res}")
    else:
        # לוגיקה אוטומטית (נחזיר אותה ברגע שהבדיקה תעבור)
        now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
        print(f"Automatic run at hour: {now.hour}")

if __name__ == "__main__":
    main()
