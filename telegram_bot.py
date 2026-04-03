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
            "text": text[:4000],
            "parse_mode": "Markdown"
        }, timeout=10)
    except Exception:
        pass

def run_report():
    send_msg("⏳ *מריץ דוח מלא... רגע אחד!*")
    try:
        result = subprocess.run(
            ["python", "main.py"],
            capture_output=True, text=True, timeout=300,
            env={**os.environ, "GITHUB_EVENT_NAME": "workflow_dispatch"}
        )
        if result.returncode != 0:
            err = result.stderr[-800:] if result.stderr else "Unknown error"
            send_msg(f"❌ *שגיאה ב-main.py:*\n`{err}`")
        else:
            send_msg("✅ *הדוח נשלח בהצלחה!*")
    except subprocess.TimeoutExpired:
        send_msg("⚠️ *Timeout — הדוח לקח יותר מ-5 דקות*")
    except Exception as e:
        send_msg(f"❌ *Exception:* `{str(e)[:300]}`")

def main():
    # שלב 1: אפס offset לפי עדכונים קיימים (אל תריץ אותם)
    offset = None
    try:
        updates = get_updates()
        if updates:
            offset = updates[-1]["update_id"] + 1
    except Exception:
        pass

    print(f"🤖 Bot polling — offset={offset}")

    # שלב 2: polling למשך 50 שניות
    deadline = time.time() + 50

    while time.time() < deadline:
        updates = get_updates(offset)

        for upd in updates:
            offset = upd["update_id"] + 1
            msg     = upd.get("message", {})
            text    = msg.get("text", "").strip()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            # אבטחה: רק CHAT_ID המאושר
            if chat_id != CHAT_ID:
                print(f"⛔ Blocked chat_id={chat_id}")
                continue

            print(f"📩 Received: {text} from {chat_id}")

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

        time.sleep(3)

if __name__ == "__main__":
    main()
