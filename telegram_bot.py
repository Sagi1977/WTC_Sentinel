import os
import time
import requests
import subprocess

TOKEN   = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = str(os.environ.get('TELEGRAM_CHAT_ID', ''))
BASE    = f"https://api.telegram.org/bot{TOKEN}"

def get_updates(offset=None):
    params = {"timeout": 10, "allowed_updates": ["message"]}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(f"{BASE}/getUpdates", params=params, timeout=15)
        return r.json().get("result", [])
    except Exception:
        return []

def send_msg(text):
    try:
        requests.post(f"{BASE}/sendMessage", json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }, timeout=10)
    except Exception:
        pass

def run_report():
    send_msg("⏳ *מריץ דוח מלא... רגע אחד!*")
    result = subprocess.run(
        ["python", "main.py"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        send_msg(f"❌ *שגיאה:*\n`{result.stderr[:500]}`")

def main():
    # קרא עדכונים קיימים רק כדי לאפס את ה-offset — אל תריץ עליהם
    offset = None
    updates = get_updates()
    if updates:
        offset = updates[-1]["update_id"] + 1

    print(f"🤖 Bot polling — offset={offset}")

    # polling למשך 55 שניות (timeout יכרות את ה-Action)
    deadline = time.time() + 55

    while time.time() < deadline:
        updates = get_updates(offset)

        for upd in updates:
            offset = upd["update_id"] + 1
            msg     = upd.get("message", {})
            text    = msg.get("text", "").strip()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            # אבטחה: רק מה-CHAT_ID המאושר
            if chat_id != CHAT_ID:
                continue

            print(f"📩 Received: {text}")

            if text in ("/start", "/report", "/דוח"):
                run_report()

            elif text == "/help":
                send_msg(
                    "📋 *פקודות זמינות:*\n"
                    "/start — דוח מלא עכשיו\n"
                    "/report — דוח מלא עכשיו\n"
                    "/דוח — דוח מלא עכשיו\n"
                    "/help — עזרה"
                )

        time.sleep(5)

if __name__ == "__main__":
    main()
