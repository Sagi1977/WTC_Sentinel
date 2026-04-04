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

def get_updates(offset=None, timeout=20):
    params = {"timeout": timeout, "allowed_updates": ["message"]}
    if offset is not None:
        params["offset"] = offset
    try:
        r = requests.get(f"{BASE}/getUpdates", params=params, timeout=timeout + 5)
        return r.json().get("result", [])
    except Exception:
        return []

def get_latest_offset():
    """מחזיר את ה-update_id של ההודעה האחרונה — בלי למחוק כלום"""
    try:
        r = requests.get(f"{BASE}/getUpdates", params={"timeout": 0}, timeout=10)
        results = r.json().get("result", [])
        if results:
            return results[-1]["update_id"]
        return None
    except Exception:
        return None

def ack(offset):
    """מסמן הודעה כנקראה — מונע עיבוד כפול בריצה הבאה"""
    get_updates(offset=offset, timeout=0)

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

    # ── שלב 1: בדוק מה קיים בתור ──────────────────────────────────
    latest = get_latest_offset()

    if latest is None:
        # תור ריק — האזן להודעות חדשות בלבד
        offset = None
        print("Queue empty — listening for new messages")
    else:
        # יש הודעות — התחל מהאחרונה (כולל אותה)
        offset = latest
        print(f"Queue has messages, starting from offset={offset}")

    # ── שלב 2: polling 45 שניות ────────────────────────────────────
    deadline = time.time() + 45

    while time.time() < deadline:
        remaining = int(deadline - time.time())
        if remaining < 5:
            break

        poll_timeout = min(20, remaining - 3)
        updates = get_updates(offset=offset, timeout=poll_timeout)

        for upd in updates:
            offset = upd["update_id"] + 1
            msg     = upd.get("message", {})
            text    = msg.get("text", "").strip().lower()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            print(f"Received: chat_id={chat_id} text={text!r}")

            if chat_id != CHAT_ID:
                print(f"Ignored: {chat_id}")
                ack(offset)
                continue

            if text in ("/start", "/report", "/דוח", "start", "report"):
                ack(offset)       # ← סמן כנקרא לפני הריצה הארוכה
                run_report()
            elif text in ("/help", "help"):
                send_msg("📋 *פקודות:*\n/start — דוח מלא\n/help — עזרה")
                ack(offset)
            else:
                send_msg(f"❓ לא מכיר: `{text}`\nשלח /help לרשימת הפקודות.")
                ack(offset)

    print(f"Bot finished, final offset={offset}")

if __name__ == "__main__":
    main()
