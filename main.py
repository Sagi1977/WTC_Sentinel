import os
import time
import io
import re
import requests
import pandas as pd
import yfinance as yf
import pytz
import datetime
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
GEMINI_KEY = os.environ.get("GEMINI_API_KEY")
BASE = f"https://api.telegram.org/bot{TOKEN}"


def send_msg(text):
    if not text:
        return
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        try:
            requests.post(
                f"{BASE}/sendMessage",
                json={"chat_id": CHAT_ID, "text": chunk, "parse_mode": "Markdown"},
                timeout=10,
            )
        except Exception:
            pass
        time.sleep(0.5)


def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)


def download_latest_file(service, prefix):
    try:
        res = service.files().list(
            q=f"name contains '{prefix}'", orderBy="createdTime desc"
        ).execute()
        files = res.get("files", [])
        if not files:
            return None, "❓ Missing"
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]["id"])).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh, encoding="utf-8-sig", engine="python"), "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"


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
    except Exception:
        return None


def filter_rth(df):
    if df is None or df.empty:
        return df
    try:
        idx = df.index
        et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
        mask = (((et_idx.hour == 9) & (et_idx.minute >= 30)) |
                ((et_idx.hour > 9) & (et_idx.hour < 16)))
        return df[mask]
    except Exception:
        return df


def get_5m_rth(ticker, period="1d"):
    try:
        raw = yf.download(ticker, period=period, interval="5m", progress=False)
        return filter_rth(raw)
    except Exception:
        return None


def find_open_at_or_after(df, target_hour, target_minute, return_ts=False):
    if df is None or df.empty:
        return (None, None) if return_ts else None
    open_s = extract_col(df, "Open")
    if open_s is None or open_s.empty:
        return (None, None) if return_ts else None
    idx = df.index
    et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            value = float(open_s.iloc[i])
            return (value, ts) if return_ts else value
    return (None, None) if return_ts else None


def get_monday_10am_open_debug(ticker):
    try:
        df = get_5m_rth(ticker, period="5d")
        if df is None or df.empty:
            return None, "no_5d_data"
        et_idx = df.index.tz_convert("America/New_York") if (hasattr(df.index, "tz") and df.index.tz) else df.index
        monday_mask = [ts.weekday() == 0 for ts in et_idx]
        monday_df = df[monday_mask]
        if monday_df is None or monday_df.empty:
            return None, "no_monday_rows"
        wk_open, wk_ts = find_open_at_or_after(monday_df, 10, 0, return_ts=True)
        if wk_open is None:
            return None, "no_bar_after_10am"
        return wk_open, wk_ts.strftime("%Y-%m-%d %H:%M ET")
    except Exception as e:
        return None, f"err:{str(e)[:20]}"


def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
            df = df.rename(columns=clean)
            sel = next((c for c in df.columns if "final" in c or "selection" in c), None)
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


def get_market_dashboard():
    try:
        spy_2d = yf.download("SPY", period="2d", interval="1d", progress=False)
        spy_cls = extract_col(spy_2d, "Close")
        s_p = float(spy_cls.iloc[-1])
        prev_c = float(spy_cls.iloc[-2])
        s_c = ((s_p / prev_c) - 1) * 100
        v_p = float(yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1])
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return (
            f"📊 *WTC Sentinel Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n"
            f"`--------------------------`\n"
        )
    except Exception:
        return "⚠️ Dashboard Offline\n\n"


def get_portfolio_performance(watchlist):
    if not watchlist:
        return "⚠️ Watchlist empty\n"
    report = "📈 *My Portfolio Watch (Dynamic)*\n"
    report += "`Type | Ticker | Price | Day% | Wk% | Status`\n"
    report += "`--------------------------------------------------`\n"
    debug = "🛠 *Wk Debug*\n"
    debug += "`Ticker | WkBase | Source`\n"
    debug += "`-----------------------------------------------`\n"
    for t, label in watchlist.items():
        try:
            d2 = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2 = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2:
                report += f"`{'N/D':<8} | {t:<5} | N/A | N/A | N/A | ⚠️`\n"
                debug += f"`{t:<5} | {'N/A':<8} | no_2d_close`\n"
                continue
            curr_p = float(cls_d2.iloc[-1])
            prev_p = float(cls_d2.iloc[-2])
            day_chg = ((curr_p / prev_p) - 1) * 100
            wk_open, wk_source = get_monday_10am_open_debug(t)
            if wk_open is None:
                wk_open = prev_p
                wk_source = f"fallback_prev_close {prev_p:.2f}"
            wk_chg = ((curr_p / wk_open) - 1) * 100
            status = "✅ Break" if wk_chg >= 0 else "❌ Below"
            lbl = (label[:7] + ".") if len(label) > 8 else label[:8]
            report += (
                f"`{lbl:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )
            debug += f"`{t:<5} | {wk_open:>6.2f} | {wk_source[:28]:<28}`\n"
        except Exception as e:
            report += f"`{'Err':<8} | {t:<5} | N/A | N/A | N/A | ❌`\n"
            debug += f"`{t:<5} | {'N/A':<8} | err:{str(e)[:22]}`\n"
    report += "`--------------------------------------------------`\n\n"
    debug += "`-----------------------------------------------`\n"
    return report + debug + "\n"


def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get("title") or n.get("content", {}).get("title")
                if title:
                    news += f"- {title}\n"
        except Exception:
            continue
    prompt = custom_prompt if custom_prompt else (
        f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
        f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n"
        f"### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    )
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if "flash" in m.name), "gemini-1.5-flash")
        return client.models.generate_content(model=target, contents=prompt).text
    except Exception:
        return "⚠️ AI Summary Unavailable"


def build_underdog_list(service):
    underdogs = []
    for prefix, bucket in [("Golden_Plan_STOCKS", "STOCKS"), ("Golden_Plan_ETF", "ETF")]:
        df, _ = download_latest_file(service, prefix)
        if df is None:
            continue
        clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
        df = df.rename(columns=clean)
        sel = next((c for c in df.columns if "final" in c or "selection" in c), None)
        tcol = next((c for c in df.columns if "ticker" in c), "ticker")
        scol = next((c for c in df.columns if "score" in c), None)
        if not sel or tcol not in df.columns:
            continue
        mask = ~df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
        for _, row in df[mask].iterrows():
            t = str(row[tcol]).strip().upper()
            score = row.get(scol, "N/A") if scol else "N/A"
            if t:
                underdogs.append((t, bucket, score))
    return underdogs


def run_execution_scan(service):
    underdogs = build_underdog_list(service)
    res = {"STOCKS": [], "ETF": []}
    debug = []
    for t, bucket, score in underdogs:
        try:
            d2 = yf.download(t, period="2d", interval="1d", progress=False)
            cls_d2 = extract_col(d2, "Close")
            if cls_d2 is None or len(cls_d2) < 2:
                debug.append((t, "N/A", "no_2d_close"))
                continue
            curr_p = float(cls_d2.iloc[-1])
            prev_p = float(cls_d2.iloc[-2])
            day_chg = ((curr_p / prev_p) - 1) * 100
            wk_open, wk_source = get_monday_10am_open_debug(t)
            if wk_open is None:
                debug.append((t, "N/A", wk_source))
                continue
            wk_chg = ((curr_p / wk_open) - 1) * 100
            debug.append((t, f"{wk_open:.2f}", wk_source))
            if wk_chg > 5:
                res[bucket].append((t, curr_p, day_chg, wk_chg, score))
        except Exception as e:
            debug.append((t, "N/A", f"err:{str(e)[:20]}"))
            continue
    res["STOCKS"].sort(key=lambda x: x[3], reverse=True)
    res["ETF"].sort(key=lambda x: x[3], reverse=True)
    total = len(res["STOCKS"]) + len(res["ETF"])
    v_p = float(yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1])
    report = "🎯 *Execution Scan — UnderRadar*\n"
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
    report += "\n🛠 *Execution Wk Debug*\n"
    report += "`Ticker | WkBase | Source`\n"
    report += "`-----------------------------------------------`\n"
    for t, wk_base, src in debug[:30]:
        report += f"`{t:<5} | {wk_base:<6} | {src[:28]:<28}`\n"
    report += "`-----------------------------------------------`\n\n"
    if total == 0:
        report += "💡 *סיכום:* אין Underdogs עם Wk% מעל +5% כרגע."
        if v_p > 22:
            report += " VIX גבוה — זהירות."
    else:
        report += f"🚀 *סיכום:* {total} הזדמנויות מתחת לרדאר עם Wk% > +5%."
    return report


def main():
    service = get_drive_service()
    watchlist, drive_logs = build_dynamic_watchlist(service)
    dashboard = get_market_dashboard()
    dashboard += f"\n🔍 *Diagnostics:*\n`{drive_logs}`\n"
    portfolio = get_portfolio_performance(watchlist)
    ai_report = get_ai_report()
    execution_scan = run_execution_scan(service)

    send_msg(f"{dashboard}\n{portfolio}")
    send_msg(ai_report)
    send_msg(execution_scan)


if __name__ == "__main__":
    main()
