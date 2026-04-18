import os
import time
import io
import re
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
BASE = f"https://api.telegram.org/bot{TOKEN}"


def send_msg(text):
    if not text:
        return
    for chunk in [text[i:i + 3800] for i in range(0, len(text), 3800)]:
        try:
            requests.post(
                f"{BASE}/sendMessage",
                json={"chat_id": CHAT_ID, "text": chunk},
                timeout=15,
            )
        except Exception as e:
            print(f"TELEGRAM_ERROR: {str(e)[:120]}")
        time.sleep(0.4)


def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)


def download_latest_file(service, prefix):
    try:
        res = service.files().list(q=f"name contains '{prefix}'", orderBy="createdTime desc").execute()
        files = res.get("files", [])
        if not files:
            return None, "Missing"
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]["id"])).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh, encoding="utf-8-sig", engine="python"), "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:80]}"


def extract_col(df, col_name):
    if df is None or getattr(df, "empty", False):
        return None
    try:
        if isinstance(df.columns, pd.MultiIndex):
            lvl = df.columns.get_level_values(0)
            if col_name not in lvl:
                return None
            result = df[col_name]
        else:
            if col_name not in df.columns:
                return None
            result = df[col_name]
        if isinstance(result, pd.DataFrame):
            if result.shape[1] == 0:
                return None
            result = result.iloc[:, 0]
        if np.isscalar(result):
            result = pd.Series([result])
        return result
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
        raw = yf.download(ticker, period=period, interval="5m", progress=False, auto_adjust=False)
        return filter_rth(raw)
    except Exception:
        return None


def get_latest_rth_session(ticker, period="5d"):
    try:
        df = get_5m_rth(ticker, period=period)
        if df is None or df.empty:
            return None
        et_idx = df.index.tz_convert("America/New_York") if (hasattr(df.index, "tz") and df.index.tz) else df.index
        session_dates = pd.Series(et_idx.date, index=df.index)
        last_date = session_dates.iloc[-1]
        return df[session_dates == last_date]
    except Exception:
        return None


def find_open_at_or_after(df, target_hour=9, target_minute=30):
    if df is None or getattr(df, "empty", False):
        return None
    open_s = extract_col(df, "Open")
    if open_s is None or getattr(open_s, "empty", False):
        return None
    et_idx = df.index.tz_convert("America/New_York") if (hasattr(df.index, "tz") and df.index.tz) else df.index
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            try:
                return float(open_s.iloc[i])
            except Exception:
                return None
    return None


def get_week_start_open(ticker):
    try:
        df = get_5m_rth(ticker, period="1mo")
        if df is None or df.empty:
            return None
        et_idx = df.index.tz_convert("America/New_York") if (hasattr(df.index, "tz") and df.index.tz) else df.index
        week_keys = pd.Index([d.isocalendar()[:2] for d in pd.to_datetime(et_idx).date])
        current_week = week_keys[-1]
        week_df = df[week_keys == current_week]
        if week_df is None or week_df.empty:
            return None
        week_et_idx = week_df.index.tz_convert("America/New_York") if (hasattr(week_df.index, "tz") and week_df.index.tz) else week_df.index
        session_dates = pd.Series(week_et_idx.date, index=week_df.index)
        first_session = week_df[session_dates == session_dates.iloc[0]]
        return find_open_at_or_after(first_session, 9, 30)
    except Exception:
        return None


def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is None:
            logs.append(f"❌ {prefix}: {status}")
            continue
        clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
        df = df.rename(columns=clean)
        sel = next((c for c in df.columns if "final" in c or "selection" in c), None)
        tcol = next((c for c in df.columns if "ticker" in c), "ticker")
        if not sel or tcol not in df.columns:
            logs.append(f"⚠️ {prefix}: Col missing")
            continue
        mask = df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
        for _, row in df[mask].iterrows():
            tick = str(row[tcol]).strip().upper()
            if tick:
                watchlist[tick] = str(row[sel])
        logs.append(f"✅ {prefix}: Found {int(mask.sum())}")
    return watchlist, "\n".join(logs)


def get_market_dashboard():
    try:
        spy_2d = yf.download("SPY", period="2d", interval="1d", progress=False)
        spy_cls = extract_col(spy_2d, "Close")
        if spy_cls is None or len(spy_cls) < 2:
            return "⚠️ Dashboard Offline\n"
        s_p = float(spy_cls.iloc[-1])
        prev_c = float(spy_cls.iloc[-2])
        s_c = ((s_p / prev_c) - 1) * 100
        v_hist = yf.Ticker("^VIX").history(period="1d")
        v_p = float(v_hist["Close"].iloc[-1])
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return (
            f"📊 WTC Sentinel Dashboard\n"
            f"--------------------------\n"
            f"Status: {status} {emoji}\n"
            f"VIX: {v_p:.2f} | SPY: {s_p:.2f} ({s_c:+.2f}%)\n"
            f"--------------------------\n"
        )
    except Exception:
        return "⚠️ Dashboard Offline\n"


def get_portfolio_performance(watchlist):
    if not watchlist:
        return "⚠️ Watchlist empty\n"
    report = "My Portfolio Watch (Dynamic)\n"
    report += "Type | Ticker | Price | Day% | Wk% | Status\n"
    report += "--------------------------------------------------\n"
    for t, label in watchlist.items():
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            if session_df is None or session_df.empty or close_s is None or open_s is None or close_s.empty or open_s.empty:
                report += f"N/D     | {t:<5} | N/A | N/A | N/A | ⚠️\n"
                continue
            curr_p = float(close_s.iloc[-1])
            day_open = float(open_s.iloc[0])
            day_chg = ((curr_p / day_open) - 1) * 100
            prev_close_df = yf.download(t, period="2d", interval="1d", progress=False)
            prev_close_s = extract_col(prev_close_df, "Close")
            prev_p = float(prev_close_s.iloc[-2]) if prev_close_s is not None and len(prev_close_s) >= 2 else curr_p
            wk_open = get_week_start_open(t) or prev_p
            wk_chg = ((curr_p / wk_open) - 1) * 100
            if wk_chg >= 8 and day_chg >= 1:
                status = "✅ Str"
            elif wk_chg >= 3 and day_chg >= 0:
                status = "👀 Bld"
            elif wk_chg >= 0 or day_chg > -2:
                status = "⚠️ Weak"
            else:
                status = "❌ Bel"
            lbl = (label[:7] + ".") if len(label) > 8 else label[:8]
            report += f"{lbl:<8} | {t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}\n"
        except Exception:
            report += f"Err     | {t:<5} | N/A | N/A | N/A | ❌\n"
    report += "--------------------------------------------------\n"
    return report


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
            if t:
                score = row.get(scol, "N/A") if scol else "N/A"
                underdogs.append((t, bucket, score))
    return underdogs


def run_execution_scan(service, limit=20):
    underdogs = build_underdog_list(service)
    rows = []
    spy_session = get_latest_rth_session("SPY", period="5d")
    spy_close = extract_col(spy_session, "Close")
    spy_day_chg = ((float(spy_close.iloc[-1]) / float(spy_close.iloc[0])) - 1) * 100 if spy_close is not None and len(spy_close) > 1 else 0.0
    for t, bucket, score in underdogs:
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            volume_s = extract_col(session_df, "Volume")
            if session_df is None or session_df.empty or close_s is None or open_s is None or close_s.empty or open_s.empty:
                continue
            curr_p = float(close_s.iloc[-1])
            day_open = float(open_s.iloc[0])
            day_chg = ((curr_p / day_open) - 1) * 100
            wk_open = get_week_start_open(t)
            if wk_open is None:
                continue
            wk_chg = ((curr_p / wk_open) - 1) * 100
            if wk_chg <= 5:
                continue
            avg_vol = float(volume_s.mean()) if volume_s is not None and not volume_s.empty else 0.0
            rvol = float(volume_s.iloc[-1] / avg_vol) if avg_vol > 0 else 0.0
            rs = day_chg - spy_day_chg
            vwap = float((volume_s * close_s).sum() / volume_s.sum()) if volume_s is not None and volume_s.sum() > 0 else curr_p
            vwap_pct = ((curr_p / vwap) - 1) * 100
            if hasattr(close_s, "diff") and len(close_s) >= 15:
                delta = close_s.diff()
                gain = delta.clip(lower=0).rolling(window=14).mean()
                loss = (-delta.clip(upper=0)).rolling(window=14).mean()
                rs_i = gain / loss
                rsi = 100 - (100 / (1 + rs_i.iloc[-1])) if not pd.isna(rs_i.iloc[-1]) else 50
            else:
                rsi = 50
            if (wk_chg >= 15) or (vwap_pct >= 1.5 and (rsi >= 60 or rvol >= 1.5)):
                status = "⚠️ Ext"
            elif rs > 0 and rvol >= 1.2 and rsi >= 55 and -0.5 <= vwap_pct <= 1.5:
                status = "🚀 Brk"
            elif wk_chg >= 5 and (rs > 0 or rsi >= 55 or vwap_pct > -1.0):
                status = "👀 Wch"
            else:
                status = "❌ Bel"
            rows.append((t, bucket, curr_p, day_chg, wk_chg, score, rvol, rs, vwap_pct, rsi, status))
        except Exception:
            continue
    rows.sort(key=lambda x: x[4], reverse=True)
    rows = rows[:limit]
    report = "Execution Scan — UnderRadar\n"
    report += "Ticker | Price | Day% | Wk% | Score | RVol | RS | VWAP% | RSI | Status\n"
    report += "-------------------------------------------------------------------------------------\n"
    if not rows:
        report += "None\n"
        return report
    for t, bucket, p, d, w, sc, rvol, rs, vwap, rsi, st in rows:
        score_txt = sc if isinstance(sc, str) else f"{float(sc):.0f}"
        report += f"{t:<6} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {score_txt:<5} | {rvol:>4.1f}x | {rs:>+4.1f} | {vwap:>+4.1f}% | {rsi:>3.0f} | {st}\n"
    return report


def debug_execution_scan(service):
    underdogs = build_underdog_list(service)
    total_underdogs = len(underdogs)
    no_session = 0
    no_price_data = 0
    no_week_open = 0
    wk_below_5 = 0
    added = 0
    for t, bucket, score in underdogs:
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            if session_df is None or session_df.empty:
                no_session += 1
                continue
            if close_s is None or open_s is None or close_s.empty or open_s.empty:
                no_price_data += 1
                continue
            curr_p = float(close_s.iloc[-1])
            wk_open = get_week_start_open(t)
            if wk_open is None:
                no_week_open += 1
                continue
            wk_chg = ((curr_p / wk_open) - 1) * 100
            if wk_chg <= 5:
                wk_below_5 += 1
                continue
            added += 1
        except Exception:
            pass
    return (
        "UnderRadar Debug\n"
        f"Underdogs total : {total_underdogs}\n"
        f"No session      : {no_session}\n"
        f"No price data   : {no_price_data}\n"
        f"No week open    : {no_week_open}\n"
        f"Wk% <= 5        : {wk_below_5}\n"
        f"Added to scan   : {added}\n"
    )


def main():
    service = get_drive_service()
    watchlist, drive_logs = build_dynamic_watchlist(service)
    dashboard = get_market_dashboard() + "\nDiagnostics:\n" + drive_logs + "\n"
    portfolio = get_portfolio_performance(watchlist)
    execution_scan = run_execution_scan(service, limit=20)
    debug_scan = debug_execution_scan(service)
    send_msg(dashboard + "\n" + portfolio)
    send_msg(execution_scan)
    send_msg(debug_scan)


if __name__ == "__main__":
    main()
