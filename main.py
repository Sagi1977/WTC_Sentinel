import os
import datetime
import time
import pandas as pd
import yfinance as yf
import requests
import pytz
import re
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai
import google.auth
import io

TOKEN      = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID    = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
IS_MANUAL  = os.environ.get("IS_MANUAL", "false").lower() == "true"
BASE       = f"https://api.telegram.org/bot{TOKEN}"

def send_msg(text):
    if not text:
        return
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(f"{BASE}/sendMessage",
                json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "Markdown"}, timeout=10)
        except Exception:
            pass
        time.sleep(0.5)

def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)

def download_latest_file(service, prefix):
    try:
        res   = service.files().list(
            q=f"name contains '{prefix}'", orderBy="createdTime desc").execute()
        files = res.get("files", [])
        if not files:
            return None, "Missing"
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]["id"])).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh, encoding="utf-8-sig", engine="python"), "Loaded"
    except Exception as e:
        return None, f"Err:{str(e)[:30]}"

def extract_col(df, col_name):
    if df is None or df.empty:
        return None
    try:
        if isinstance(df.columns, pd.MultiIndex):
            lvl = df.columns.get_level_values(0)
            if col_name not in lvl:
                return None
            result = df[col_name]
            return result.squeeze() if isinstance(result, pd.DataFrame) else result
        return df[col_name] if col_name in df.columns else None
    except:
        return None

def filter_rth(df):
    if df is None or df.empty:
        return df
    try:
        idx    = df.index
        et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
        mask   = (((et_idx.hour == 9) & (et_idx.minute >= 30)) | ((et_idx.hour > 9) & (et_idx.hour < 16)))
        return df[mask]
    except:
        return df

def get_5m_rth(ticker, period="1d"):
    try:
        return filter_rth(yf.download(ticker, period=period, interval="5m", progress=False))
    except:
        return None

def find_open_at_or_after(df, th, tm):
    if df is None or df.empty:
        return None
    opens  = extract_col(df, "Open")
    if opens is None or opens.empty:
        return None
    idx    = df.index
    et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > th or (ts.hour == th and ts.minute >= tm):
            return float(opens.iloc[i])
    return None

def get_monday_10am_open(ticker):
    try:
        df = get_5m_rth(ticker, period="5d")
        if df is None or df.empty:
            return None
        et_idx    = df.index.tz_convert("America/New_York") if (hasattr(df.index, "tz") and df.index.tz) else df.index
        monday_df = df[[ts.weekday() == 0 for ts in et_idx]]
        return find_open_at_or_after(monday_df, 10, 0)
    except:
        return None

def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean = {c: re.sub(r'[^a-zA-Z0-9_]', '_', str(c).lower()) for c in df.columns}
            df    = df.rename(columns=clean)
            sel   = next((c for c in df.columns if "final" in c or "selection" in c), None)
            tcol  = next((c for c in df.columns if "ticker" in c), "ticker")
            if sel:
                mask = df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
                for _, row in df[mask].iterrows():
                    watchlist[str(row[tcol]).strip().upper()] = str(row[sel])
                logs.append(f"{prefix}: Found {mask.sum()}")
            else:
                logs.append(f"{prefix}: Col missing")
        else:
            logs.append(f"{prefix}: {status}")
    return watchlist, "\n".join(logs)

def build_underdog_list(service):
    underdogs = []
    for prefix, bucket in [("Golden_Plan_STOCKS", "STOCKS"), ("Golden_Plan_ETF", "ETF")]:
        df, _ = download_latest_file(service, prefix)
        if df is None:
            continue
        clean = {c: re.sub(r'[^a-zA-Z0-9_]', '_', str(c).lower()) for c in df.columns}
        df    = df.rename(columns=clean)
        sel   = next((c for c in df.columns if "final" in c or "selection" in c), None)
        tcol  = next((c for c in df.columns if "ticker" in c), "ticker")
        scol  = next((c for c in df.columns if "score"  in c), None)
        if not sel or tcol not in df.columns:
            continue
        mask  = df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
        for _, row in df[mask].iterrows():
            t     = str(row[tcol]).strip().upper()
            score = row.get(scol, "N/A") if scol else "N/A"
            underdogs.append((t, bucket, score))
    return underdogs

def get_market_dashboard():
    try:
        spy2d   = yf.download("SPY", period="2d", interval="1d", progress=False)
        spy_cls = extract_col(spy2d, "Close")
        s_p     = float(spy_cls.iloc[-1])
        prev_c  = float(spy_cls.iloc[-2])
        s_c     = (s_p / prev_c - 1) * 100
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
    except:
        return "⚠️ Dashboard Offline\n\n"

def get_portfolio_performance(watchlist):
    if not watchlist:
        return "⚠️ Watchlist empty\n"
    report  = "📈 *My Portfolio Watch (Dynamic)*\n"
    report += "`Type     | Ticker | Price  | Day%  | Wk%   | Status`\n"
    report += "`--------------------------------------------------`\n"
    for t, label in watchlist.items():
        try:
            d2      = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2  = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2:
                report += f"`{'ND':<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️`\n"
                continue
            curr_p  = float(cls_d2.iloc[-1])
            prev_p  = float(cls_d2.iloc[-2])
            day_chg = (curr_p / prev_p - 1) * 100
            wk_open = get_monday_10am_open(t) or prev_p
            wk_chg  = (curr_p / wk_open - 1) * 100
            status  = "✅ Break" if wk_chg >= 0 else "❌ Below"
            lbl     = (label[:7] + ".") if len(label) > 8 else label[:8]
            report += f"`{lbl:<8} | {t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
        except:
            report += f"`{'Err':<8} | {t:<5} | N/A    | N/A   | N/A   | ❌`\n"
    report += "`--------------------------------------------------`\n"
    return report + "\n"

def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get("title") or n.get("content", {}).get("title")
                if title:
                    news += f"- {title}\n"
        except:
            continue
    prompt = custom_prompt or (
        f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
        f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n"
        f"### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    )
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if "flash" in m.name), "gemini-1.5-flash")
        return client.models.generate_content(model=target, contents=prompt).text
    except:
        return "⚠️ AI Unavailable"

def run_execution_scan(service):
    underdogs = build_underdog_list(service)
    report    = "🎯 *Execution Scan*\n"

    # Golden Plan status
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        report += f"**{prefix}:** {status}\n"

    report += "\n🥇 *STOCKS:* "
    stocks = []
    for t, bucket, score in underdogs:
        if bucket != "STOCKS":
            continue
        try:
            d2     = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2 = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2:
                continue
            curr_p  = float(cls_d2.iloc[-1])
            prev_p  = float(cls_d2.iloc[-2])
            day_chg = (curr_p / prev_p - 1) * 100
            wk_open = get_monday_10am_open(t)
            if wk_open is None:
                continue
            wk_chg  = (curr_p / wk_open - 1) * 100
            stocks.append((t, score, curr_p, day_chg, wk_chg))
        except:
            continue

    if stocks:
        stocks.sort(key=lambda x: x[4], reverse=True)
        for t, score, p, d, w in stocks:
            report += f"`{t}({score})` "
    else:
        report += "`None`"

    report += "\n\n🏅 *ETF:* "
    etfs = []
    for t, bucket, score in underdogs:
        if bucket != "ETF":
            continue
        try:
            d2     = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2 = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2:
                continue
            curr_p  = float(cls_d2.iloc[-1])
            prev_p  = float(cls_d2.iloc[-2])
            day_chg = (curr_p / prev_p - 1) * 100
            wk_open = get_monday_10am_open(t)
            if wk_open is None:
                continue
            wk_chg  = (curr_p / wk_open - 1) * 100
            etfs.append((t, score, curr_p, day_chg, wk_chg))
        except:
            continue

    if etfs:
        etfs.sort(key=lambda x: x[4], reverse=True)
        for t, score, p, d, w in etfs:
            report += f"`{t}({score})` "
    else:
        report += "`None`"

    return report + "\n"

def send_full_report(service, watchlist, drive_logs):
    db        = get_market_dashboard()
    db       += f"\n🔍 *Diagnostics:*\n`{drive_logs}`\n"
    portfolio = get_portfolio_performance(watchlist)
    send_msg(f"{db}\n{portfolio}")
    send_msg(get_ai_report())
    send_msg(run_execution_scan(service))

def main():
    service                = get_drive_service()
    isr_tz                 = pytz.timezone("Asia/Jerusalem")
    now                    = datetime.datetime.now(datetime.timezone.utc).astimezone(isr_tz)
    hour, minute           = now.hour, now.minute
    watchlist, drive_logs  = build_dynamic_watchlist(service)

    if IS_MANUAL:
        send_full_report(service, watchlist, drive_logs)
        return

    if hour == 16 and minute <= 35:
        db  = get_market_dashboard()
        db += f"\n🔍 *Diagnostics:*\n`{drive_logs}`\n"
        send_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour <= 20 and minute <= 35:
        db  = get_market_dashboard()
        db += f"\n🔍 *Diagnostics:*\n`{drive_logs}`\n"
        send_msg(f"{db}\n{get_portfolio_performance(watchlist)}")
        send_msg(run_execution_scan(service))
    elif hour == 23 and minute <= 35:
        db      = get_market_dashboard()
        closing = "סכם בעברית את יום המסחר בוול סטריט עבור סוחר מקצועי."
        send_msg(f"{db}\n🌙 *Closing Summary*\n\n{get_ai_report(closing)}")
    else:
        print(f"No slot. Hour={hour}:{minute:02d} IST")

if __name__ == "__main__":
    main()
