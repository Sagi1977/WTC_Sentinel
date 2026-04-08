import os
import time
import io
import re
import math
import requests
import pandas as pd
import yfinance as yf
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
    for chunk in [text[i:i + 4000] for i in range(0, len(text), 4000)]:
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
            q=f"name contains '{prefix}'",
            orderBy="createdTime desc",
            pageSize=1,
            fields="files(id,name,createdTime)",
        ).execute()
        files = res.get("files", [])
        if not files:
            return None, "❓ Missing"

        fh = io.BytesIO()
        request = service.files().get_media(fileId=files[0]["id"])
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
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
        mask = (
            ((et_idx.hour == 9) & (et_idx.minute >= 30)) |
            ((et_idx.hour > 9) & (et_idx.hour < 16))
        )
        return df[mask]
    except Exception:
        return df


def get_5m_rth(ticker, period="10d"):
    try:
        raw = yf.download(ticker, period=period, interval="5m", progress=False, auto_adjust=False)
        return filter_rth(raw)
    except Exception:
        return None


def get_daily_history(ticker, period="3mo"):
    try:
        return yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    except Exception:
        return None


def compute_rsi(series, period=14):
    if series is None or len(series) < period + 1:
        return None
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    return None if pd.isna(val) else float(val)


def compute_intraday_vwap(df):
    close_s = extract_col(df, "Close")
    high_s = extract_col(df, "High")
    low_s = extract_col(df, "Low")
    vol_s = extract_col(df, "Volume")
    if any(x is None for x in [close_s, high_s, low_s, vol_s]):
        return None
    if close_s.empty or vol_s.empty:
        return None
    typical = (high_s + low_s + close_s) / 3.0
    cum_vol = vol_s.cumsum()
    if float(cum_vol.iloc[-1]) <= 0:
        return None
    vwap = (typical * vol_s).cumsum() / cum_vol
    return float(vwap.iloc[-1])


def get_spy_day_change():
    try:
        metrics = get_intraday_session_metrics("SPY")
        return None if metrics is None else metrics["day_change"]
    except Exception:
        return None


def get_intraday_session_metrics(ticker):
    try:
        df = get_5m_rth(ticker, period="10d")
        if df is None or df.empty:
            return None

        idx = df.index
        et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
        dates = pd.Series(et_idx.date, index=df.index)
        unique_dates = list(pd.unique(dates))
        if not unique_dates:
            return None

        latest_day = unique_dates[-1]
        today_df = df[(dates == latest_day).values].copy()
        if today_df.empty:
            return None

        open_today = extract_col(today_df, "Open")
        close_today = extract_col(today_df, "Close")
        high_today = extract_col(today_df, "High")
        vol_today = extract_col(today_df, "Volume")
        if any(x is None for x in [open_today, close_today, high_today, vol_today]):
            return None
        if open_today.empty or close_today.empty or high_today.empty or vol_today.empty:
            return None

        day_open = float(open_today.iloc[0])
        current_price = float(close_today.iloc[-1])
        day_change = ((current_price / day_open) - 1) * 100
        today_volume = float(vol_today.sum())
        vwap = compute_intraday_vwap(today_df)
        above_vwap = None if vwap is None else current_price >= vwap

        week_start = latest_day - datetime.timedelta(days=latest_day.weekday())
        week_dates = [d for d in unique_dates if week_start <= d <= latest_day]
        if not week_dates:
            return None

        first_trading_day = week_dates[0]
        week_df = df[dates.isin(week_dates).values].copy()
        first_day_df = df[(dates == first_trading_day).values].copy()
        if week_df.empty or first_day_df.empty:
            return None

        open_week = extract_col(first_day_df, "Open")
        high_week = extract_col(week_df, "High")
        if open_week is None or high_week is None or open_week.empty or high_week.empty:
            return None

        week_open = float(open_week.iloc[0])
        week_change = ((current_price / week_open) - 1) * 100

        prior_high = None
        if len(high_week) > 1:
            prior_high = float(high_week.iloc[:-1].max())
        elif len(high_week) == 1:
            prior_high = float(high_week.iloc[0])

        daily_hist = get_daily_history(ticker, period="3mo")
        rel_volume = None
        rsi14 = None
        if daily_hist is not None and not daily_hist.empty:
            vol_d = extract_col(daily_hist, "Volume")
            close_d = extract_col(daily_hist, "Close")
            if vol_d is not None:
                vol_d = vol_d.dropna()
                if len(vol_d) >= 20:
                    avg20 = float(vol_d.tail(20).mean())
                    if avg20 > 0:
                        rel_volume = today_volume / avg20
            if close_d is not None:
                close_d = close_d.dropna()
                if len(close_d) >= 20:
                    rsi14 = compute_rsi(close_d, period=14)

        return {
            "current_price": current_price,
            "day_open": day_open,
            "day_change": day_change,
            "week_open": week_open,
            "week_change": week_change,
            "prior_high": prior_high,
            "today_volume": today_volume,
            "rel_volume": rel_volume,
            "vwap": vwap,
            "above_vwap": above_vwap,
            "rsi14": rsi14,
        }
    except Exception:
        return None


def get_monday_10am_open(ticker):
    metrics = get_intraday_session_metrics(ticker)
    return None if metrics is None else metrics["week_open"]


def fmt_pct(v):
    return "N/A" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:+.1f}%"


def fmt_num(v, digits=1):
    return "N/A" if v is None or (isinstance(v, float) and math.isnan(v)) else f"{v:.{digits}f}"


def yes_no(v):
    if v is None:
        return "N/A"
    return "Y" if v else "N"


def classify_portfolio_status(day_chg, wk_chg):
    if wk_chg >= 2.0 and day_chg >= 0.7:
        return "✅ Strong"
    if wk_chg >= 1.0 and day_chg >= 0.2:
        return "👀 Building"
    if wk_chg >= 0 or day_chg >= 0:
        return "⚠️ Weak"
    return "❌ Below"


def classify_execution_status(day_chg, wk_chg, current_price, prior_high, rel_volume, rs_vs_spy, above_vwap, rsi14):
    breakout = current_price > (prior_high * 1.001) if prior_high is not None else False
    relvol = rel_volume if rel_volume is not None else -1
    rs = rs_vs_spy if rs_vs_spy is not None else -999
    rsi = rsi14 if rsi14 is not None else -1
    vwap_ok = above_vwap is True

    if wk_chg >= 7.0 and day_chg >= 3.0 and relvol >= 1.2 and rs >= 1.0 and rsi >= 60:
        return "⚠️ Extended"

    if breakout and wk_chg >= 3.0 and day_chg >= 0.8 and relvol >= 1.0 and rs >= 0.5 and vwap_ok and rsi >= 58:
        return "🚀 Breakout"

    if wk_chg >= 2.0 and day_chg >= 0.3 and relvol >= 1.0 and rs >= 0.5 and rsi >= 55:
        return "👀 Watch"

    return "❌ Below"


def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
            df = df.rename(columns=clean)
            sel = next((c for c in df.columns if "final" in c or "selection" in c), None)
            tcol = next((c for c in df.columns if "ticker" in c), "ticker")
            if sel and tcol in df.columns:
                mask = df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
                for _, row in df[mask].iterrows():
                    ticker = str(row[tcol]).strip().upper()
                    if ticker:
                        watchlist[ticker] = str(row[sel])
                logs.append(f"✅ {prefix}: Found {int(mask.sum())}")
            else:
                logs.append(f"⚠️ {prefix}: Col missing")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)


def get_market_dashboard():
    try:
        spy_2d = yf.download("SPY", period="2d", interval="1d", progress=False, auto_adjust=False)
        spy_cls = extract_col(spy_2d, "Close")
        if spy_cls is None or len(spy_cls) < 2:
            return "⚠️ Dashboard Offline\n\n"
        s_p = float(spy_cls.iloc[-1])
        prev_c = float(spy_cls.iloc[-2])
        s_c = ((s_p / prev_c) - 1) * 100
        vix_hist = yf.Ticker("^VIX").history(period="1d")
        if vix_hist is None or vix_hist.empty:
            return "⚠️ Dashboard Offline\n\n"
        v_p = float(vix_hist["Close"].iloc[-1])
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

    for t, label in watchlist.items():
        try:
            metrics = get_intraday_session_metrics(t)
            if metrics is None:
                report += f"`{'N/D':<8} | {t:<5} | N/A | N/A | N/A | ⚠️`\n"
                continue

            curr_p = metrics["current_price"]
            day_chg = metrics["day_change"]
            wk_chg = metrics["week_change"]
            status = classify_portfolio_status(day_chg, wk_chg)
            lbl = (label[:7] + ".") if len(label) > 8 else label[:8]

            report += (
                f"`{lbl:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )
        except Exception:
            report += f"`{'Err':<8} | {t:<5} | N/A | N/A | N/A | ❌`\n"

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
        except Exception:
            continue

    prompt = custom_prompt if custom_prompt else (
        f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
        f"מבנה: ## דוח אסטרטגי\n"
        f"### 🏛️ 1. הכסף הגדול\n"
        f"### 💣 2. מוקשים ומאקרו\n"
        f"### 🌡️ 3. סנטימנט"
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
            ticker = str(row[tcol]).strip().upper()
            score = row.get(scol, "N/A") if scol else "N/A"
            if ticker:
                underdogs.append((ticker, bucket, score))
    return underdogs


def run_execution_scan(service):
    underdogs = build_underdog_list(service)
    res = {"STOCKS": [], "ETF": []}
    spy_day_chg = get_spy_day_change()

    for t, bucket, score in underdogs:
        try:
            metrics = get_intraday_session_metrics(t)
            if metrics is None:
                continue

            curr_p = metrics["current_price"]
            day_chg = metrics["day_change"]
            wk_chg = metrics["week_change"]
            rel_volume = metrics["rel_volume"]
            above_vwap = metrics["above_vwap"]
            rsi14 = metrics["rsi14"]
            rs_vs_spy = None if spy_day_chg is None else (day_chg - spy_day_chg)

            status = classify_execution_status(
                day_chg=day_chg,
                wk_chg=wk_chg,
                current_price=curr_p,
                prior_high=metrics["prior_high"],
                rel_volume=rel_volume,
                rs_vs_spy=rs_vs_spy,
                above_vwap=above_vwap,
                rsi14=rsi14,
            )

            if status != "❌ Below":
                res[bucket].append((
                    t, curr_p, day_chg, wk_chg, score, rel_volume, rs_vs_spy, above_vwap, rsi14, status
                ))
        except Exception:
            continue

    res["STOCKS"].sort(key=lambda x: x[3], reverse=True)
    res["ETF"].sort(key=lambda x: x[3], reverse=True)
    total = len(res["STOCKS"]) + len(res["ETF"])

    try:
        v_p = float(yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1])
    except Exception:
        v_p = None

    report = "🎯 *Execution Scan — UnderRadar*\n"
    report += "`--------------------------------------------------------------------------`\n\n"

    report += "🥇 *STOCKS:*\n"
    if res["STOCKS"]:
        report += "`Ticker | Price | Day% | Wk% | Score | RVol | RS | VWAP | RSI | Status`\n"
        report += "`--------------------------------------------------------------------------`\n"
        for t, p, d, w, sc, rv, rs, av, rsi, st in res["STOCKS"]:
            report += (
                f"`{t:<5} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {str(sc):<5} | "
                f"{fmt_num(rv, 1):>4} | {fmt_pct(rs):>5} | {yes_no(av):>4} | {fmt_num(rsi, 0):>3} | {st}`\n"
            )
    else:
        report += "_None_\n"

    report += "\n🏅 *ETF:*\n"
    if res["ETF"]:
        report += "`Ticker | Price | Day% | Wk% | Score | RVol | RS | VWAP | RSI | Status`\n"
        report += "`--------------------------------------------------------------------------`\n"
        for t, p, d, w, sc, rv, rs, av, rsi, st in res["ETF"]:
            report += (
                f"`{t:<5} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {str(sc):<5} | "
                f"{fmt_num(rv, 1):>4} | {fmt_pct(rs):>5} | {yes_no(av):>4} | {fmt_num(rsi, 0):>3} | {st}`\n"
            )
    else:
        report += "_None_\n"

    report += "\n"
    if total == 0:
        report += "💡 *סיכום:* אין כרגע מועמדות UnderRadar עם אישור חי מספיק חזק."
        if v_p is not None and v_p > 22:
            report += " VIX גבוה — זהירות."
    else:
        if v_p is not None and v_p > 22:
            report += f"⚠️ *סיכום:* נמצאו {total} מועמדות, אבל VIX גבוה ולכן עדיף זהירות בביצוע."
        else:
            report += f"🚀 *סיכום:* נמצאו {total} מועמדות UnderRadar עם אישור חי של מומנטום, חוזק יחסי ונפח."

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
