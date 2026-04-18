import os
import time
import io
import re
import requests
import pandas as pd
import numpy as np
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
    if df is None or getattr(df, "empty", False):
        return None
    try:
        if not hasattr(df, "columns"):
            return None
        if isinstance(df.columns, pd.MultiIndex):
            lvl = df.columns.get_level_values(0)
            if col_name not in lvl:
                return None
            result = df[col_name]
            if isinstance(result, pd.DataFrame):
                if result.shape[1] == 0:
                    return None
                result = result.iloc[:, 0]
            if np.isscalar(result):
                result = pd.Series([result])
            return result
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
        idx = df.index
        et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
        session_dates = pd.Series(et_idx.date, index=df.index)
        last_date = session_dates.iloc[-1]
        return df[session_dates == last_date]
    except Exception:
        return None

def find_open_at_or_after(df, target_hour, target_minute):
    if df is None or getattr(df, "empty", False):
        return None
    open_s = extract_col(df, "Open")
    if open_s is None or getattr(open_s, "empty", False):
        return None
    idx = df.index
    et_idx = idx.tz_convert("America/New_York") if (hasattr(idx, "tz") and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            try:
                return float(open_s.iloc[i])
            except Exception:
                try:
                    return float(open_s.iloc[-1])
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
        week_mask = week_keys == current_week
        week_df = df[week_mask]
        if week_df is None or week_df.empty:
            return None
        week_et_idx = week_df.index.tz_convert("America/New_York") if (hasattr(week_df.index, "tz") and week_df.index.tz) else week_df.index
        session_dates = pd.Series(week_et_idx.date, index=week_df.index)
        first_date = session_dates.iloc[0]
        first_session = week_df[session_dates == first_date]
        return find_open_at_or_after(first_session, 9, 30)
    except Exception:
        return None

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
    for t, label in watchlist.items():
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            if session_df is None or session_df.empty or close_s is None or close_s.empty or open_s is None or open_s.empty:
                report += f"`{'N/D':<8} | {t:<5} | N/A | N/A | N/A | ⚠️`\n"
                continue

            curr_p = float(close_s.iloc[-1])
            day_open = float(open_s.iloc[0])
            day_chg = ((curr_p / day_open) - 1) * 100

            prev_close_df = yf.download(t, period="2d", interval="1d", progress=False)
            prev_close_s = extract_col(prev_close_df, "Close")
            prev_p = float(prev_close_s.iloc[-2]) if prev_close_s is not None and len(prev_close_s) >= 2 else curr_p

            wk_open = get_week_start_open(t)
            if wk_open is None:
                wk_open = prev_p
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
            report += (
                f"`{lbl:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )

        except Exception:
            report += f"`{'Err':<8} | {t:<5} | N/A | N/A | N/A | ❌`\n"
    report += "`--------------------------------------------------`\n"
    return report + "\n"

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
    for t, bucket, score in underdogs:
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            volume_s = extract_col(session_df, "Volume")
            if (session_df is None or session_df.empty or close_s is None or close_s.empty or 
                open_s is None or open_s.empty):
                continue

            curr_p = float(close_s.iloc[-1])
            day_open = float(open_s.iloc[0])
            day_chg = ((curr_p / day_open) - 1) * 100

            wk_open = get_week_start_open(t)
            if wk_open is None:
                continue
            wk_chg = ((curr_p / wk_open) - 1) * 100
            if wk_chg > 5:
                # RVol - נפח יחסי
                avg_vol = volume_s.mean() if volume_s is not None else 1.0
                rvol = volume_s.iloc[-1] / avg_vol if avg_vol > 0 else 0.0

                # RS vs SPY
                spy_session = get_latest_rth_session("SPY", period="5d")
                spy_close = extract_col(spy_session, "Close")
                spy_day_chg = (spy_close.iloc[-1] / spy_close.iloc[0] - 1) * 100 if spy_close is not None and len(spy_close) > 1 else 0.0
                rs = day_chg - spy_day_chg

                # VWAP
                vwap = (volume_s * close_s).sum() / volume_s.sum() if volume_s.sum() > 0 else curr_p
                vwap_pct = ((curr_p / vwap) - 1) * 100

                # RSI(14)
                if hasattr(close_s, "diff") and len(close_s) >= 15:
                    delta = close_s.diff()
                    gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                    rs_i = gain / loss
                    rsi = 100 - (100 / (1 + rs_i.iloc[-1])) if not pd.isna(rs_i.iloc[-1]) else 50
                else:
                    rsi = 50

                # Status for Execution Scan (signal-oriented)
                if (wk_chg >= 15) or (vwap_pct >= 1.5 and (rsi >= 60 or rvol >= 1.5)):
                    status = "⚠️ Ext"
                elif rs > 0 and rvol >= 1.2 and rsi >= 55 and -0.5 <= vwap_pct <= 1.5:
                    status = "🚀 Brk"
                elif wk_chg >= 5 and (rs > 0 or rsi >= 55 or vwap_pct > -1.0):
                    status = "👀 Wch"
                else:
                    status = "❌ Bel"

                res[bucket].append((t, curr_p, day_chg, wk_chg, score, rvol, rs, vwap_pct, rsi, status))
        except Exception:
            continue

    res["STOCKS"].sort(key=lambda x: x[3], reverse=True)
    res["ETF"].sort(key=lambda x: x[3], reverse=True)
    total = len(res["STOCKS"]) + len(res["ETF"])
    v_p = float(yf.Ticker("^VIX").history(period="1d")["Close"].iloc[-1])
    report = "🎯 *Execution Scan — UnderRadar*\n"
    report += "`Ticker | Price | Day% | Wk% | Score | RVol | RS | VWAP% | RSI | Status`\n"
    report += "`-------------------------------------------------------------------------------------`\n\n"

    report += "🥇 *STOCKS:*\n"
    if res["STOCKS"]:
        for t, p, d, w, sc, rvol, rs, vwap, rsi, st in res["STOCKS"]:
            report += f"`{t:<6} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {sc:<5} | {rvol:>4.1f}x | {rs:>+4.1f} | {vwap:>+4.1f}% | {rsi:>3.0f} | {st}`\n"
    else:
        report += "_None_\n"

    report += "\n🏅 *ETF:*\n"
    if res["ETF"]:
        for t, p, d, w, sc, rvol, rs, vwap, rsi, st in res["ETF"]:
            report += f"`{t:<6} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | {sc:<5} | {rvol:>4.1f}x | {rs:>+4.1f} | {vwap:>+4.1f}% | {rsi:>3.0f} | {st}`\n"
    else:
        report += "_None_\n"

    report += "\n"
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
    execution_scan = run_execution_scan(service)

    send_msg(f"{dashboard}\n{portfolio}")
    send_msg(execution_scan)

if __name__ == "__main__":
    main()


def compute_execution_rows(service):
    underdogs = build_underdog_list(service)
    rows = {"STOCKS": [], "ETF": []}
    for t, bucket, score_str in underdogs:
        try:
            session_df = get_latest_rth_session(t, period="20d")
            if session_df is None or session_df.empty:
                continue
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            volume_s = extract_col(session_df, "Volume")
            high_s = extract_col(session_df, "High")
            low_s = extract_col(session_df, "Low")
            if any(x is None or getattr(x, "empty", False) for x in [close_s, open_s, volume_s, high_s, low_s]):
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
            rvol = float(volume_s.iloc[-1] / avg_vol) if avg_vol and avg_vol > 0 else 1.0
            spy_session = get_latest_rth_session("SPY", period="5d")
            spy_close = extract_col(spy_session, "Close")
            if spy_close is None or len(spy_close) < 2:
                continue
            spy_day_chg = ((float(spy_close.iloc[-1]) / float(spy_close.iloc[0])) - 1) * 100
            rs = day_chg - spy_day_chg
            vwap_base = volume_s.rolling(5).sum()
            vwap_num = (volume_s * close_s).rolling(5).sum()
            vwap_last = float(vwap_num.iloc[-1] / vwap_base.iloc[-1]) if float(vwap_base.iloc[-1]) > 0 else curr_p
            vwap_pct = ((curr_p / vwap_last) - 1) * 100
            delta = close_s.diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs_i = gain / loss
            rsi = float(100 - (100 / (1 + rs_i.iloc[-1]))) if not pd.isna(rs_i.iloc[-1]) else 50.0
            score = float(score_str) if score_str != "N/A" and str(score_str) != "nan" else 50.0
            status = "Ext" if wk_chg >= 15 or (rvol >= 2 and rsi < 80 and vwap_pct > 0) else ("Brk" if rs > 0 and rvol >= 1.2 and rsi >= 55 else ("Wch" if wk_chg >= 5 and (rs > 0 or rsi >= 55 or vwap_pct > -1.0) else "Bel"))
            rank = (3 if status == "Ext" else 2 if status == "Brk" else 1 if status == "Wch" else 0) * 10 + score / 10 + wk_chg / 5 + (100 - rsi) / 20 + (2 if rvol >= 2 else 0)
            rows[bucket].append({
                "ticker": t, "price": curr_p, "day": day_chg, "wk": wk_chg,
                "score": score, "rvol": rvol, "rs": rs, "vwap": vwap_pct,
                "rsi": rsi, "status": status, "rank": round(rank, 2)
            })
        except Exception:
            continue
    for b in rows:
        rows[b] = sorted(rows[b], key=lambda x: x["rank"], reverse=True)[:10]
    return rows


def build_execution_report(service):
    rows = compute_execution_rows(service)
    report = "🎯 *Execution Scan — UnderRadar*\\n"
    report += "`Ticker | Price | Day% | Wk% | Score | RVol | RS | VWAP% | RSI | Status | Rank`\\n"
    report += "`----------------------------------------------------------------------------------------------------`\\n\\n"

    report += "🥇 *STOCKS:*\\n"
    if rows["STOCKS"]:
        for row in rows["STOCKS"]:
            report += (
                f"`{row['ticker']:<6} | "
                f"{row['price']:>6.2f} | "
                f"{row['day']:>+5.1f}% | "
                f"{row['wk']:>+5.1f}% | "
                f"{row['score']:>5.1f} | "
                f"{row['rvol']:>4.1f}x | "
                f"{row['rs']:>+4.1f} | "
                f"{row['vwap']:>+4.1f}% | "
                f"{row['rsi']:>3.0f} | "
                f"{row['status']:<4} | "
                f"{row['rank']:>5.2f}`\\n"
            )
    else:
        report += "_None_\\n"

    report += "\\n🏅 *ETF:*\\n"
    if rows["ETF"]:
        for row in rows["ETF"]:
            report += (
                f"`{row['ticker']:<6} | "
                f"{row['price']:>6.2f} | "
                f"{row['day']:>+5.1f}% | "
                f"{row['wk']:>+5.1f}% | "
                f"{row['score']:>5.1f} | "
                f"{row['rvol']:>4.1f}x | "
                f"{row['rs']:>+4.1f} | "
                f"{row['vwap']:>+4.1f}% | "
                f"{row['rsi']:>3.0f} | "
                f"{row['status']:<4} | "
                f"{row['rank']:>5.2f}`\\n"
            )
    else:
        report += "_None_\\n"

    return report


def run_execution_scan_v2(service):
    return build_execution_report(service)

def run_execution_scan_v2(service):
    return build_execution_report(service)

def run_execution_scan_v2(service):
    return build_execution_report(service)


USE_EXECUTION_SCAN_V2 = True

def main_v2():
    service = get_drive_service()
    watchlist, drive_logs = build_dynamic_watchlist(service)
    dashboard = get_market_dashboard()
    portfolio = get_portfolio_performance(watchlist)
    execution_scan = run_execution_scan_v2(service) if USE_EXECUTION_SCAN_V2 else run_execution_scan(service)
    msg1 = dashboard + "\n🔍 Diagnostics:\n" + drive_logs + "\n" + portfolio
    send_msg(msg1)
    send_msg(execution_scan)

if __name__ == "__main__":
    main_v2()
