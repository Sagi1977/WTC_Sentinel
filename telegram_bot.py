import os
import time
import requests
import subprocess

TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
BASE    = f"https://api.telegram.org/bot{TOKEN}"

def send_msg(text):
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(f"{BASE}/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": chunk,
                "parse_mode": "Markdown"
            }, timeout=10)
        except Exception:
            pass
        time.sleep(0.3)

def get_updates(offset=None):
    params = {"timeout": 20, "allowed_updates": ["message"]}
    if offset:
        params["offset"] = offset
    try:
        r = requests.get(f"{BASE}/getUpdates", params=params, timeout=25)
        return r.json().get("result", [])
    except Exception:
        return []

def run_report():
    send_msg("⏳ *מריץ דוח מלא... 30-60 שניות...*")
    try:
        result = subprocess.run(
            ["python", "main.py"],
            capture_output=True, text=True, timeout=300,
            env={**os.environ, "GITHUB_EVENT_NAME": "workflow_dispatch"}
        )
        if result.returncode != 0:
            err = (result.stderr or "")[-800:]
            send_msg(f"❌ *שגיאה:*\n`{err}`")
        else:
            print("main.py finished OK")
    except subprocess.TimeoutExpired:
        send_msg("⚠️ *Timeout — הדוח לקח יותר מ-5 דקות*")
    except Exception as e:
        send_msg(f"❌ *Exception:* `{str(e)[:300]}`")

def main():
    print(f"Bot starting — CHAT_ID={CHAT_ID}")

    # offset=None — קורא את כל ההודעות הממתינות ומעבד אותן
    offset = None
    deadline = time.time() + 45

    while time.time() < deadline:
        if deadline - time.time() < 5:
            break

        updates = get_updates(offset)

        for upd in updates:
            offset = upd["update_id"] + 1
            msg     = upd.get("message", {})
            text    = msg.get("text", "").strip().lower()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            print(f"Received: chat_id={chat_id} text={text!r}")

            if chat_id != CHAT_ID:
                print(f"Ignored unknown chat_id={chat_id}")
                continue

            if text in ("/start", "/report", "/דוח", "start", "report"):
                run_report()
            elif text in ("/help", "help"):
                send_msg("📋 *פקודות:*\n/start — דוח מלא\n/help — עזרה")
            else:
                send_msg(f"❓ לא מכיר: `{text}`\nשלח /help לרשימת הפקודות.")

    print(f"Bot finished, final offset={offset}")

if __name__ == "__main__":
    main()
