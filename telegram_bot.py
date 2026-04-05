import os
import time
import datetime
import pandas as pd
import yfinance as yf
import requests
import pytz
import re
from flask import Flask, request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai
import google.auth
import io
import threading

TOKEN      = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID    = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
BASE       = f"https://api.telegram.org/bot{TOKEN}"

app = Flask(__name__)

# ─── Telegram helpers ────────────────────────────────────────────
def send_msg(text):
    if not text: return
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(f"{BASE}/sendMessage",
                json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "Markdown"},
                timeout=10)
        except Exception:
            pass
        time.sleep(0.5)

# ─── Google Drive ─────────────────────────────────────────────────
def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)

def download_latest_file(service, prefix):
    try:
        res = service.files().list(
            q=f"name contains '{prefix}'", orderBy="createdTime desc").execute()
        files = res.get("files", [])
        if not files: return None, "❓ Missing"
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]["id"])).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh, encoding="utf-8-sig", engine="python"), "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

# ─── Data helpers ─────────────────────────────────────────────────
def extract_col(df, col_name):
    if df is None or df.empty: return None
    try:
        if isinstance(df.columns, pd.MultiIndex):
            lvl = df.columns.get_level_values(0)
            if col_name not in lvl: return None
            result = df[col_name]
            return result.squeeze() if isinstance(result, pd.DataFrame) else result
        return df[col_name] if col_name in df.columns else None
    except: return None

def filter_rth(df):
    if df is None or df.empty: return df
    try:
        idx = df.index
        et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
        mask = (((et_idx.hour == 9) & (et_idx.minute >= 30)) |
                ((et_idx.hour > 9) & (et_idx.hour < 16)))
        return df[mask]
    except: return df

def get_5m_rth(ticker, period="1d"):
    try:
        raw = yf.download(ticker, period=period, interval="5m", progress=False)
        return filter_rth(raw)
    except: return None

def find_open_at_or_after(df, target_hour, target_minute):
    if df is None or df.empty: return None
    open_s = extract_col(df, "Open")
    if open_s is None or open_s.empty: return None
    idx = df.index
    et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            return float(open_s.iloc[i])
    return None

def get_monday_10am_open(ticker):
    try:
        df = get_5m_rth(ticker, period="5d")
        if df is None or df.empty: return None
        et_idx = df.index.tz_convert("America/New_York") if (hasattr(df.index, "tz") and df.index.tz) else df.index
        monday_df = df[[ts.weekday() == 0 for ts in et_idx]]
        return find_open_at_or_after(monday_df, 10, 0)
    except: return None

# ─── 1. Watchlist ─────────────────────────────────────────────────
def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
            df = df.rename(columns=clean)
            sel  = next((c for c in df.columns if "final" in c or "selection" in c), None)
            tcol = next((c for c in df.columns if "ticker" in c), "ticker")
            if sel:
                mask = df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
                for _, row in df[mask].iterrows():
                    watchlist[str(row[tcol]).strip().upper()] = str(row[sel])
                logs.append(f"✅ {prefix}: Found {mask.sum()}")
            else:
                logs.append(f"⚠️ {prefix}: Col missing")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# ─── 2. Dashboard ─────────────────────────────────────────────────
def get_market_dashboard():
    try:
        spy_2d  = yf.download("SPY", period="2d", interval="1d", progress=False)
        spy_cls = extract_col(spy_2d, "Close")
        s_p     = float(spy_cls.iloc[-1])
        prev_c  = float(spy_cls.iloc[-2])
        s_c     = ((s_p / prev_c) - 1) * 100
        v_p     = float(yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1])
        status  = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji   = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return (
            f"📊 *WTC Sentinel Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n"
            f"`--------------------------`\n"
        )
    except: return "⚠️ Dashboard Offline\n\n"

# ─── 3. Portfolio ─────────────────────────────────────────────────
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist empty\n"
    report  = "📈 *My Portfolio Watch (Dynamic)*\n"
    report += "`Type | Ticker | Price | Day% | Wk% | Status`\n"
    report += "`--------------------------------------------------`\n"
    for t, label in watchlist.items():
        try:
            d2     = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2 = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2:
                report += f"`{'N/D':<8} | {t:<5} | N/A | N/A | N/A | ⚠️`\n"
                continue
            curr_p  = float(cls_d2.iloc[-1])
            prev_p  = float(cls_d2.iloc[-2])
            day_chg = ((curr_p / prev_p) - 1) * 100
            wk_open = get_monday_10am_open(t)
            if wk_open is None: wk_open = prev_p
            wk_chg  = ((curr_p / wk_open) - 1) * 100
            status  = "✅ Break" if wk_chg >= 0 else "❌ Below"
            lbl     = (label[:7] + ".") if len(label) > 8 else label[:8]
            report += (
                f"`{lbl:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )
        except:
            report += f"`{'Err':<8} | {t:<5} | N/A | N/A | N/A | ❌`\n"
    report += "`--------------------------------------------------`\n"
    return report + "\n"

# ─── 4. AI Report ────────────────────────────────────────────────
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get("title") or n.get("content", {}).get("title")
                if title: news += f"- {title}\n"
        except: continue
    prompt = custom_prompt if custom_prompt else (
        f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
        f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n"
        f"### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    )
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if "flash" in m.name), "gemini-1.5-flash")
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ AI Summary Unavailable"

# ─── 5. UnderDogs ────────────────────────────────────────────────
def build_underdog_list(service):
    underdogs = []
    for prefix, bucket in [("Golden_Plan_STOCKS", "STOCKS"), ("Golden_Plan_ETF", "ETF")]:
        df, _ = download_latest_file(service, prefix)
        if df is None: continue
        clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
        df    = df.rename(columns=clean)
        sel   = next((c for c in df.columns if "final" in c or "selection" in c), None)
        tcol  = next((c for c in df.columns if "ticker" in c), "ticker")
        scol  = next((c for c in df.columns if "score" in c), None)
        if not sel or tcol not in df.columns: continue
        mask  = ~df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
        for _, row in df[mask].iterrows():
            t     = str(row[tcol]).strip().upper()
            score = row.get(scol, "N/A") if scol else "N/A"
            if t: underdogs.append((t, bucket, score))
    return underdogs

# ─── 6. Execution Scan ───────────────────────────────────────────
def run_execution_scan(service):
    underdogs = build_underdog_list(service)
    res = {"STOCKS": [], "ETF": []}
    for t, bucket, score in underdogs:
        try:
            d2     = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2 = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2: continue
            curr_p  = float(cls_d2.iloc[-1])
            prev_p  = float(cls_d2.iloc[-2])
            day_chg = ((curr_p / prev_p) - 1) * 100
            wk_open = get_monday_10am_open(t)
            if wk_open is None: continue
            wk_chg  = ((curr_p / wk_open) - 1) * 100
            if wk_chg > 5:
                res[bucket].append((t, curr_p, day_chg, wk_chg, score))
        except: continue
    res["STOCKS"].sort(key=lambda x: x[3], reverse=True)
    res["ETF"].sort(key=lambda x: x[3], reverse=True)
    total = len(res["STOCKS"]) + len(res["ETF"])
    v_p   = float(yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1])
    report  = "🎯 *Execution Scan — UnderRadar*\n"
    report += "`-----------------------------`\n\n"
    report += "🥇 *STOCKS:*\n"
    if res["STOCKS"]:
        report += "`Ticker | Price | Day% | Wk% | Score`\n"
        report += "`-----------------------------------`\n"
        for t, p, d, w, sc in res["STOCKS"]:
            report += f"`{t:<5} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {str(sc):<5}`\n"
    else:
        report += "_None_\n"
    report += "\n🏅 *ETF:*\n"
    if res["ETF"]:
        report += "`Ticker | Price | Day% | Wk% | Score`\n"
        report += "`-----------------------------------`\n"
        for t, p, d, w, sc in res["ETF"]:
            report += f"`{t:<5} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {str(sc):<5}`\n"
    else:
        report += "_None_\n"
    report += "\n"
    if total == 0:
        report += "💡 *סיכום:* אין Underdogs עם Wk% מעל +5% כרגע."
        if v_p > 22: report += " VIX גבוה — זהירות."
    else:
        report += f"🚀 *סיכום:* {total} הזדמנויות מתחת לרדאר עם Wk% > +5%."
    return report

# ─── Full report ─────────────────────────────────────────────────
def send_full_report():
    try:
        service = get_drive_service()
        watchlist, drive_logs = build_dynamic_watchlist(service)
        db = get_market_dashboard()
        db += f"\n🔍 *Diagnostics:*\n`{drive_logs}`\n"
        portfolio = get_portfolio_performance(watchlist)
        send_msg(f"{db}\n{portfolio}")
        send_msg(get_ai_report())
        send_msg(run_execution_scan(service))
    except Exception as e:
        send_msg(f"❌ שגיאה בדוח: {str(e)}")

# ─── Webhook ─────────────────────────────────────────────────────
@app.route(f"/webhook/{TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json()
    msg  = data.get("message", {})
    text = msg.get("text", "").strip().lower()
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if chat_id == CHAT_ID and text in ("/start", "/report", "/דוח", "start", "report"):
        send_msg("✅ קיבלתי! מכין דוח מלא...")
        threading.Thread(target=send_full_report).start()
    return "ok", 200

@app.route("/")
def index():
    return "WTC Sentinel Bot is running ✅", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
