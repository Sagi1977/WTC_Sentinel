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
TOP_N = 10
SHOW_DEBUG = False


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


def get_5m_rth(ticker, period="5d"):
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
            logs.append(f"{prefix}: {status}")
            continue
        clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
        df = df.rename(columns=clean)
        sel = next((c for c in df.columns if "final" in c or "selection" in c), None)
        tcol = next((c for c in df.columns if "ticker" in c), "ticker")
        if not sel or tcol not in df.columns:
            logs.append(f"{prefix}: Col missing")
            continue
        mask = df[sel].astype(str).str.contains("Anchor|Turbo|Top 5", na=False, case=False)
        for _, row in df[mask].iterrows():
            tick = str(row[tcol]).strip().upper()
            if tick:
                watchlist[tick] = str(row[sel])
        logs.append(f"{prefix}: Found {int(mask.sum())}")
    return watchlist, "\n".join(logs)


def get_market_dashboard():
    try:
        spy_2d = yf.download("SPY", period="2d", interval="1d", progress=False)
        spy_cls = extract_col(spy_2d, "Close")
        if spy_cls is None or len(spy_cls) < 2:
            return "WTC Sentinel Dashboard\nStatus: Offline\n"
        s_p = float(spy_cls.iloc[-1])
        prev_c = float(spy_cls.iloc[-2])
        s_c = ((s_p / prev_c) - 1) * 100
        v_hist = yf.Ticker("^VIX").history(period="1d")
        v_p = float(v_hist["Close"].iloc[-1])
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        return (
            "WTC Sentinel Dashboard\n"
            f"Status: {status}\n"
            f"VIX: {v_p:.2f} | SPY: {s_p:.2f} ({s_c:+.2f}%)\n"
        )
    except Exception:
        return "WTC Sentinel Dashboard\nStatus: Offline\n"


def get_portfolio_performance(watchlist):
    if not watchlist:
        return "Portfolio Watch\nWatchlist empty\n"
    lines = []
    lines.append("Portfolio Watch")
    lines.append("Type   | Ticker | Price  | Day%  | Wk%   | Status")
    lines.append("---------------------------------------------------")
    for t, label in watchlist.items():
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            if session_df is None or session_df.empty or close_s is None or open_s is None or close_s.empty or open_s.empty:
                lines.append(f"N/D    | {t:<6} | N/A    | N/A   | N/A   | Weak")
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
                status = "Str"
            elif wk_chg >= 3 and day_chg >= 0:
                status = "Bld"
            elif wk_chg >= 0 or day_chg > -2:
                status = "Weak"
            else:
                status = "Bel"
            lbl = (label[:6] + ".") if len(label) > 7 else label[:7]
            lines.append(f"{lbl:<6} | {t:<6} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}")
        except Exception:
            lines.append(f"Err    | {t:<6} | N/A    | N/A   | N/A   | Bel")
    return "\n".join(lines)


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


def compute_execution_rows(service):
    underdogs = build_underdog_list(service)
    all_rows = []
    spy_session = get_latest_rth_session("SPY", period="5d")
    spy_close = extract_col(spy_session, "Close")
    spy_day_chg = ((float(spy_close.iloc[-1]) / float(spy_close.iloc[0])) - 1) * 100 if spy_close is not None and len(spy_close) > 1 else 0.0

    for t, bucket, score_str in underdogs:
        try:
            session_df = get_latest_rth_session(t, period="20d")
            if session_df is None or session_df.empty:
                continue
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            volume_s = extract_col(session_df, "Volume")
            if any(x is None or getattr(x, "empty", False) for x in [close_s, open_s, volume_s]):
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

            avg_vol = volume_s.rolling(10).mean().iloc[-1]
            rvol = float(volume_s.iloc[-1] / avg_vol) if pd.notna(avg_vol) and avg_vol > 0 else 1.0
            rs = day_chg - spy_day_chg
            vwap_base = volume_s.rolling(5).sum()
            vwap_num = (volume_s * close_s).rolling(5).sum()
            vwap_last = float(vwap_num.iloc[-1] / vwap_base.iloc[-1]) if pd.notna(vwap_base.iloc[-1]) and float(vwap_base.iloc[-1]) > 0 else curr_p
            vwap_pct = ((curr_p / vwap_last) - 1) * 100
            delta = close_s.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs_i = gain / loss
            rsi = float(100 - (100 / (1 + rs_i.iloc[-1]))) if not pd.isna(rs_i.iloc[-1]) else 50.0
            score = float(score_str) if str(score_str) not in ["N/A", "nan", "None", ""] else 50.0

            if wk_chg >= 15 or (rvol >= 2 and rsi < 80 and vwap_pct > 0):
                status = "Ext"
                status_weight = 3
            elif rs > 0 and rvol >= 1.2 and rsi >= 55 and -0.5 <= vwap_pct <= 1.5:
                status = "Brk"
                status_weight = 2
            elif wk_chg >= 5 and (rs > 0 or rsi >= 55 or vwap_pct > -1.0):
                status = "Wch"
                status_weight = 1
            else:
                status = "Bel"
                status_weight = 0

            rank = (
                status_weight * 10
                + score / 10
                + wk_chg / 5
                + (100 - rsi) / 20
                + (2 if rvol >= 2 else 0)
            )

            all_rows.append({
                "ticker": t,
                "bucket": bucket,
                "price": curr_p,
                "day": day_chg,
                "wk": wk_chg,
                "score": score,
                "rvol": rvol,
                "rs": rs,
                "vwap": vwap_pct,
                "rsi": rsi,
                "status": status,
                "rank": round(rank, 2),
            })
        except Exception:
            continue

    all_rows = sorted(all_rows, key=lambda x: x["rank"], reverse=True)[:TOP_N]
    return all_rows


def build_execution_report(service):
    rows = compute_execution_rows(service)
    lines = []
    lines.append("Execution Scan — UnderRadar | TOP 10")
    lines.append("Ticker | Type   | Price  | Day%  | Wk%   | Score | RVol | RS   | VWAP% | RSI | St | Rank")
    lines.append("----------------------------------------------------------------------------------------------")
    if not rows:
        lines.append("None")
        return "\n".join(lines)
    for row in rows:
        lines.append(
            "{ticker:<6} | {bucket:<6} | {price:>6.2f} | {day:>+5.1f}% | {wk:>+5.1f}% | {score:>5.1f} | {rvol:>4.1f}x | {rs:>+4.1f} | {vwap:>+5.1f}% | {rsi:>3.0f} | {status:<3} | {rank:>5.2f}".format(**row)
        )
    return "\n".join(lines)


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
    dashboard = get_market_dashboard()
    portfolio = get_portfolio_performance(watchlist)
    execution_scan = build_execution_report(service)

    msg1 = dashboard + "\nDiagnostics:\n" + drive_logs + "\n\n" + portfolio
    send_msg(msg1)
    send_msg(execution_scan)
    if SHOW_DEBUG:
        send_msg(debug_execution_scan(service))


if __name__ == "__main__":
    main()
