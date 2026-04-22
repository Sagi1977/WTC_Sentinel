import os
import time
import io
import re
import json
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =========================================================
# 1. CONFIG
# =========================================================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", "")).strip()
BASE = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""
TOP_N = 10
SHOW_DEBUG = str(os.environ.get("SHOW_DEBUG", "false")).lower() == "true"
DRIVE_PREFIXES = ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]
SELECTION_PATTERN = r"Anchor|Turbo|Top 5"
RTH_TZ = "America/New_York"
RTH_START = (9, 30)
RTH_END_HOUR = 16

DATA_CACHE = {}
DEBUG_EVENTS = []

# =========================================================
# 2. DEBUG / LOGGING
# =========================================================
def log_event(level, where, message, **kwargs):
    payload = {"level": level, "where": where, "message": message}
    if kwargs:
        payload.update(kwargs)
    DEBUG_EVENTS.append(payload)
    if SHOW_DEBUG:
        print(f"[{level}] {where}: {message} | {kwargs if kwargs else ''}")


def get_debug_summary(limit=20):
    if not DEBUG_EVENTS:
        return "No debug events"
    tail = DEBUG_EVENTS[-limit:]
    return "\n".join(
        f"- {x['level']} | {x['where']} | {x['message']}"
        for x in tail
    )


# =========================================================
# 3. ENV / VALIDATION
# =========================================================
def validate_environment():
    errors = []
    if not TOKEN:
        errors.append("Missing TELEGRAM_TOKEN")
    if not CHAT_ID:
        errors.append("Missing TELEGRAM_CHAT_ID")
    if errors:
        raise RuntimeError("Environment validation failed: " + "; ".join(errors))
    return True


# =========================================================
# 4. TELEGRAM
# =========================================================
def send_msg(text, retries=2, sleep_seconds=0.5):
    if not text:
        log_event("WARN", "send_msg", "empty text")
        return
    if not TOKEN or not CHAT_ID:
        log_event("ERROR", "send_msg", "telegram env missing")
        return

    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        last_error = None
        for attempt in range(retries + 1):
            try:
                r = requests.post(
                    f"{BASE}/sendMessage",
                    json={"chat_id": CHAT_ID, "text": chunk},
                    timeout=15,
                )
                if r.ok:
                    log_event("INFO", "send_msg", "telegram chunk sent", status_code=r.status_code)
                    break
                last_error = f"HTTP {r.status_code}: {r.text[:200]}"
            except Exception as e:
                last_error = str(e)[:200]
            time.sleep(sleep_seconds * (attempt + 1))
        if last_error:
            log_event("ERROR", "send_msg", "failed to send chunk", error=last_error)
        time.sleep(0.3)


# =========================================================
# 5. DRIVE / OUTPUTS
# =========================================================
def get_drive_service():
    creds, _ = google.auth.default()
    return build("drive", "v3", credentials=creds)


def normalize_columns(df):
    if df is None or getattr(df, "empty", False):
        return df
    clean = {c: re.sub(r"[^a-zA-Z0-9]", "", str(c)).lower() for c in df.columns}
    return df.rename(columns=clean)


def find_selection_col(df):
    return next((c for c in df.columns if "final" in c or "selection" in c), None)


def find_ticker_col(df):
    return next((c for c in df.columns if "ticker" in c), None)


def find_score_col(df):
    return next((c for c in df.columns if "score" in c), None)


def validate_output_schema(df, prefix):
    if df is None or getattr(df, "empty", False):
        raise ValueError(f"{prefix}: dataframe empty")
    ticker_col = find_ticker_col(df)
    sel_col = find_selection_col(df)
    if not ticker_col:
        raise ValueError(f"{prefix}: missing ticker column")
    if not sel_col:
        raise ValueError(f"{prefix}: missing selection/final column")
    return ticker_col, sel_col, find_score_col(df)


def download_latest_file(service, prefix):
    try:
        res = service.files().list(
            q=f"name contains '{prefix}' and trashed=false",
            orderBy="createdTime desc",
            pageSize=5,
            fields="files(id,name,createdTime)"
        ).execute()
        files = res.get("files", [])
        if not files:
            return None, "❓ Missing"

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]["id"]))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding="utf-8-sig", engine="python")
        df = normalize_columns(df)
        validate_output_schema(df, prefix)
        return df, f"Loaded: {files[0]['name']}"
    except Exception as e:
        log_event("ERROR", "download_latest_file", "failed to load output", prefix=prefix, error=str(e)[:250])
        return None, f"Err: {str(e)[:60]}"


# =========================================================
# 6. MARKET DATA HELPERS
# =========================================================
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
    except Exception as e:
        log_event("ERROR", "extract_col", "column extraction failed", column=col_name, error=str(e)[:160])
        return None


def filter_rth(df):
    if df is None or getattr(df, "empty", False):
        return df
    try:
        idx = df.index
        et_idx = idx.tz_convert(RTH_TZ) if (hasattr(idx, "tz") and idx.tz) else idx
        mask = (((et_idx.hour == RTH_START[0]) & (et_idx.minute >= RTH_START[1])) |
                ((et_idx.hour > RTH_START[0]) & (et_idx.hour < RTH_END_HOUR)))
        return df[mask]
    except Exception as e:
        log_event("ERROR", "filter_rth", "rth filter failed", error=str(e)[:160])
        return df


def get_cached_yf_download(ticker, period, interval, auto_adjust=False):
    key = ("yf_download", ticker, period, interval, auto_adjust)
    if key in DATA_CACHE:
        return DATA_CACHE[key]
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=auto_adjust)
        DATA_CACHE[key] = df
        return df
    except Exception as e:
        log_event("ERROR", "get_cached_yf_download", "yfinance download failed", ticker=ticker, error=str(e)[:160])
        DATA_CACHE[key] = None
        return None


def get_cached_yf_history(ticker, period):
    key = ("yf_history", ticker, period)
    if key in DATA_CACHE:
        return DATA_CACHE[key]
    try:
        df = yf.Ticker(ticker).history(period=period)
        DATA_CACHE[key] = df
        return df
    except Exception as e:
        log_event("ERROR", "get_cached_yf_history", "yfinance history failed", ticker=ticker, error=str(e)[:160])
        DATA_CACHE[key] = None
        return None


def get_5m_rth(ticker, period="1d"):
    raw = get_cached_yf_download(ticker, period=period, interval="5m", auto_adjust=False)
    if raw is None:
        return None
    return filter_rth(raw)


def get_latest_rth_session(ticker, period="5d"):
    try:
        df = get_5m_rth(ticker, period=period)
        if df is None or df.empty:
            return None
        idx = df.index
        et_idx = idx.tz_convert(RTH_TZ) if (hasattr(idx, "tz") and idx.tz) else idx
        session_dates = pd.Series(et_idx.date, index=df.index)
        last_date = session_dates.iloc[-1]
        return df[session_dates == last_date]
    except Exception as e:
        log_event("ERROR", "get_latest_rth_session", "latest session failed", ticker=ticker, error=str(e)[:160])
        return None


def find_open_at_or_after(df, target_hour, target_minute):
    if df is None or getattr(df, "empty", False):
        return None
    open_s = extract_col(df, "Open")
    if open_s is None or getattr(open_s, "empty", False):
        return None
    idx = df.index
    et_idx = idx.tz_convert(RTH_TZ) if (hasattr(idx, "tz") and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            try:
                return float(open_s.iloc[i])
            except Exception:
                return None
    return None


def get_week_start_open(ticker):
    key = ("week_open", ticker)
    if key in DATA_CACHE:
        return DATA_CACHE[key]
    try:
        df = get_5m_rth(ticker, period="1mo")
        if df is None or df.empty:
            DATA_CACHE[key] = None
            return None
        et_idx = df.index.tz_convert(RTH_TZ) if (hasattr(df.index, "tz") and df.index.tz) else df.index
        week_keys = pd.Index([d.isocalendar()[:2] for d in pd.to_datetime(et_idx).date])
        current_week = week_keys[-1]
        week_mask = week_keys == current_week
        week_df = df[week_mask]
        if week_df is None or week_df.empty:
            DATA_CACHE[key] = None
            return None
        week_et_idx = week_df.index.tz_convert(RTH_TZ) if (hasattr(week_df.index, "tz") and week_df.index.tz) else week_df.index
        session_dates = pd.Series(week_et_idx.date, index=week_df.index)
        first_date = session_dates.iloc[0]
        first_session = week_df[session_dates == first_date]
        result = find_open_at_or_after(first_session, 9, 30)
        DATA_CACHE[key] = result
        return result
    except Exception as e:
        log_event("ERROR", "get_week_start_open", "week open failed", ticker=ticker, error=str(e)[:160])
        DATA_CACHE[key] = None
        return None


def safe_float(x, default=0.0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default


def calc_pct_change(current, base):
    current = safe_float(current, 0.0)
    base = safe_float(base, 0.0)
    if base <= 0:
        return 0.0
    return ((current / base) - 1) * 100


def calc_intraday_rsi(close_s, window=14):
    try:
        if close_s is None or len(close_s) < window + 1:
            return 50.0
        delta = close_s.diff()
        gain = delta.where(delta > 0, 0).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        last_gain = safe_float(gain.iloc[-1], 0.0)
        last_loss = safe_float(loss.iloc[-1], 0.0)
        if last_loss == 0 and last_gain > 0:
            return 100.0
        if last_loss == 0:
            return 50.0
        rs_i = last_gain / last_loss
        return 100 - (100 / (1 + rs_i))
    except Exception:
        return 50.0


def get_market_regime():
    try:
        spy_5d = get_cached_yf_download("SPY", period="5d", interval="1d")
        spy_close = extract_col(spy_5d, "Close")
        if spy_close is None or len(spy_close) < 2:
            return "NEUTRAL", "SPY offline"

        spy_now = float(spy_close.iloc[-1])
        spy_prev = float(spy_close.iloc[-2])
        spy_chg = calc_pct_change(spy_now, spy_prev)

        vix_hist = get_cached_yf_history("^VIX", period="5d")
        if vix_hist is None or vix_hist.empty or "Close" not in vix_hist:
            return "NEUTRAL", f"SPY {spy_chg:+.2f}% | VIX offline"

        vix_close = vix_hist["Close"]
        vix_now = float(vix_close.iloc[-1])
        vix_prev = float(vix_close.iloc[-2]) if len(vix_close) >= 2 else vix_now
        vix_chg = calc_pct_change(vix_now, vix_prev) if vix_prev else 0.0

        if vix_now >= 22 or vix_chg >= 8:
            return "EXT", f"SPY {spy_chg:+.2f}% | VIX {vix_now:.2f} ({vix_chg:+.2f}%)"
        if vix_now <= 18 and spy_chg >= 0.2:
            return "BRK/WCH", f"SPY {spy_chg:+.2f}% | VIX {vix_now:.2f} ({vix_chg:+.2f}%)"
        return "NEUTRAL", f"SPY {spy_chg:+.2f}% | VIX {vix_now:.2f} ({vix_chg:+.2f}%)"
    except Exception as e:
        log_event("ERROR", "get_market_regime", "market regime failed", error=str(e)[:160])
        return "NEUTRAL", "Regime offline"


def get_market_dashboard():
    try:
        spy_2d = get_cached_yf_download("SPY", period="2d", interval="1d")
        spy_cls = extract_col(spy_2d, "Close")
        if spy_cls is None or len(spy_cls) < 2:
            return "📊 WTC Sentinel Dashboard\n------------------------------\n⚠️ Dashboard Offline\n"

        s_p = float(spy_cls.iloc[-1])
        prev_c = float(spy_cls.iloc[-2])
        s_c = calc_pct_change(s_p, prev_c)

        vix_hist = get_cached_yf_history("^VIX", period="1d")
        if vix_hist is None or vix_hist.empty or "Close" not in vix_hist:
            return (
                "📊 WTC Sentinel Dashboard\n"
                "------------------------------\n"
                f"📉 SPY: {s_p:.2f} ({s_c:+.2f}%)\n"
                "⚠️ VIX: Offline\n"
                "------------------------------\n"
            )

        v_p = float(vix_hist["Close"].iloc[-1])
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"

        return (
            "📊 WTC Sentinel Dashboard\n"
            "------------------------------\n"
            f"🚦 Status: {status} {emoji}\n"
            f"📉 VIX: {v_p:.2f} | 📈 SPY: {s_p:.2f} ({s_c:+.2f}%)\n"
            "------------------------------\n"
        )
    except Exception as e:
        log_event("ERROR", "get_market_dashboard", "dashboard failed", error=str(e)[:160])
        return "📊 WTC Sentinel Dashboard\n------------------------------\n⚠️ Dashboard Offline\n"


# =========================================================
# 7. WATCHLIST / OUTPUT LOGIC
# =========================================================
def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in DRIVE_PREFIXES:
        df, status = download_latest_file(service, prefix)
        if df is None:
            logs.append(f"❌ {prefix}: {status}")
            continue
        try:
            tcol, sel, scol = validate_output_schema(df, prefix)
            mask = df[sel].astype(str).str.contains(SELECTION_PATTERN, na=False, case=False)
            for _, row in df[mask].iterrows():
                ticker = str(row[tcol]).strip().upper()
                if not ticker:
                    continue
                watchlist[ticker] = {
                    "label": str(row[sel]),
                    "score": row.get(scol, np.nan) if scol else np.nan,
                    "source": prefix,
                }
            logs.append(f"✅ {prefix}: Found {int(mask.sum())}")
        except Exception as e:
            logs.append(f"⚠️ {prefix}: {str(e)[:60]}")
            log_event("ERROR", "build_dynamic_watchlist", "watchlist build failed", prefix=prefix, error=str(e)[:160])
    return watchlist, "\n".join(logs)


def classify_portfolio_status(day_chg, wk_chg):
    if wk_chg >= 8 and day_chg >= 1:
        return "✅ Str", "Strong weekly and daily action"
    if wk_chg >= 3 and day_chg >= 0:
        return "👀 Bld", "Building constructively"
    if -0.5 <= wk_chg < 3 and day_chg > -1.0:
        return "🟦 Hold", "Holding near weekly base"
    if wk_chg >= 0 or day_chg > -2:
        return "⚠️ Weak", "Weak momentum / stalling"
    return "❌ Bel", "Below acceptable strength"


def get_portfolio_performance(watchlist):
    if not watchlist:
        return "📈 My Portfolio Watch (Dynamic)\n------------------------------\n⚠️ Watchlist empty\n"

    report = []
    report.append("📈 My Portfolio Watch (Dynamic)")
    report.append("--------------------------------------------------")
    report.append("Type | Ticker | Price | Day% | Wk% | Status")
    report.append("--------------------------------------------------")

    for t, info in watchlist.items():
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            if session_df is None or session_df.empty or close_s is None or close_s.empty or open_s is None or open_s.empty:
                report.append(f"{'N/D':<9} | {t:<6} | {'N/A':>6} | {'N/A':>5} | {'N/A':>5} | ⚠️")
                log_event("WARN", "get_portfolio_performance", "missing intraday data", ticker=t)
                continue

            curr_p = float(close_s.iloc[-1])
            day_open = float(open_s.iloc[0])
            day_chg = calc_pct_change(curr_p, day_open)

            prev_close_df = get_cached_yf_download(t, period="2d", interval="1d")
            prev_close_s = extract_col(prev_close_df, "Close")
            prev_p = float(prev_close_s.iloc[-2]) if prev_close_s is not None and len(prev_close_s) >= 2 else curr_p

            wk_open = get_week_start_open(t)
            if wk_open is None:
                wk_open = prev_p
            wk_chg = calc_pct_change(curr_p, wk_open)

            status, _ = classify_portfolio_status(day_chg, wk_chg)
            lbl = str(info.get("label", "")).strip()
            lbl = (lbl[:7] + ".") if len(lbl) > 8 else lbl[:8]
            report.append(
                f"{lbl:<9} | {t:<6} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}"
            )
        except Exception as e:
            report.append(f"{'Err':<9} | {t:<6} | {'N/A':>6} | {'N/A':>5} | {'N/A':>5} | ❌")
            log_event("ERROR", "get_portfolio_performance", "portfolio row failed", ticker=t, error=str(e)[:160])

    report.append("--------------------------------------------------")
    return "\n".join(report) + "\n"


def build_underdog_list(service):
    underdogs = []
    for prefix, bucket in [("Golden_Plan_STOCKS", "STOCKS"), ("Golden_Plan_ETF", "ETF")]:
        df, status = download_latest_file(service, prefix)
        if df is None:
            log_event("WARN", "build_underdog_list", "missing output", prefix=prefix, status=status)
            continue
        try:
            tcol, sel, scol = validate_output_schema(df, prefix)
            mask = ~df[sel].astype(str).str.contains(SELECTION_PATTERN, na=False, case=False)
            for _, row in df[mask].iterrows():
                t = str(row[tcol]).strip().upper()
                score = row.get(scol, np.nan) if scol else np.nan
                if t:
                    underdogs.append((t, bucket, score))
        except Exception as e:
            log_event("ERROR", "build_underdog_list", "underdog build failed", prefix=prefix, error=str(e)[:160])
    return underdogs


# =========================================================
# 8. EXECUTION SCAN
# =========================================================
def status_icon(st):
    return {"Brk": "🚀", "Wch": "👀", "Ext": "⚠️", "Bel": "❌"}.get(st, "•")


def calc_rank(sw, score_val, wk_chg, rvol, rs, vwap_pct, rsi, status):
    score_part = safe_float(score_val, 0.0) / 12.0
    week_part = max(min(wk_chg, 25.0), 0.0) / 6.0
    rs_part = max(rs, 0.0) * 1.5
    rvol_part = min(max(rvol, 0.0), 3.0)
    rsi_balance = max(0.0, 60.0 - abs(rsi - 60.0)) / 20.0
    extension_penalty = max(vwap_pct - 2.0, 0.0) * 1.5
    if status == "Ext":
        extension_penalty += max(vwap_pct - 1.0, 0.0) * 0.75
    return sw * 10 + score_part + week_part + rs_part + rvol_part + rsi_balance - extension_penalty


def classify_execution_status(regime, wk_chg, rvol, rs, vwap_pct, rsi):
    if regime == "EXT":
        if (wk_chg >= 15) or (vwap_pct >= 1.5 and (rsi >= 60 or rvol >= 1.5)):
            return "Ext", 3, "Extended under stressed regime"
        if rs > 0 and rvol >= 1.0 and rsi >= 50 and -1.0 <= vwap_pct <= 2.0:
            return "Brk", 2, "Breakout candidate in stressed regime"
        if wk_chg >= 5 and (rs > 0 or rsi >= 50 or vwap_pct > -1.5):
            return "Wch", 1, "Watch under stressed regime"
        return "Bel", 0, "Below threshold in stressed regime"

    if regime == "BRK/WCH":
        if rs > 0 and rvol >= 1.1 and rsi >= 52 and -0.5 <= vwap_pct <= 1.5:
            return "Brk", 3, "Best breakout structure"
        if wk_chg >= 5 and (rs > 0 or rsi >= 50 or vwap_pct > -1.0):
            return "Wch", 2, "Constructive watch setup"
        if (wk_chg >= 15) or (vwap_pct >= 1.5 and (rsi >= 60 or rvol >= 1.5)):
            return "Ext", 1, "Good name but extended"
        return "Bel", 0, "Below threshold"

    if rs > 0 and rvol >= 1.2 and rsi >= 55 and -0.5 <= vwap_pct <= 1.5:
        return "Brk", 2, "Breakout candidate"
    if wk_chg >= 5 and (rs > 0 or rsi >= 50 or vwap_pct > -1.0):
        return "Wch", 2, "Constructive watch setup"
    if (wk_chg >= 15) or (vwap_pct >= 1.5 and (rsi >= 60 or rvol >= 1.5)):
        return "Ext", 1, "Extended"
    return "Bel", 0, "Below threshold"


def compute_intraday_metrics(ticker, spy_day_chg=0.0):
    drop_reason = None
    session_df = get_latest_rth_session(ticker, period="5d")
    close_s = extract_col(session_df, "Close")
    open_s = extract_col(session_df, "Open")
    volume_s = extract_col(session_df, "Volume")

    if (session_df is None or session_df.empty or close_s is None or close_s.empty or
            open_s is None or open_s.empty or volume_s is None or volume_s.empty):
        return None, "missing_intraday_data"

    curr_p = safe_float(close_s.iloc[-1])
    day_open = safe_float(open_s.iloc[0], curr_p)
    if day_open <= 0:
        return None, "invalid_day_open"
    day_chg = calc_pct_change(curr_p, day_open)

    wk_open = safe_float(get_week_start_open(ticker), 0.0)
    if wk_open <= 0:
        return None, "missing_week_open"
    wk_chg = calc_pct_change(curr_p, wk_open)
    if wk_chg <= 5:
        return None, "week_change_below_threshold"

    avg_vol = safe_float(volume_s.mean(), 0.0)
    last_vol = safe_float(volume_s.iloc[-1], 0.0)
    rvol = (last_vol / avg_vol) if avg_vol > 0 else 0.0
    rs = day_chg - spy_day_chg

    vol_sum = safe_float(volume_s.sum(), 0.0)
    vwap = safe_float((volume_s * close_s).sum(), curr_p) / vol_sum if vol_sum > 0 else curr_p
    vwap_pct = calc_pct_change(curr_p, vwap) if vwap > 0 else 0.0
    rsi = calc_intraday_rsi(close_s)

    return {
        "curr_p": curr_p,
        "day_chg": day_chg,
        "wk_chg": wk_chg,
        "rvol": rvol,
        "rs": rs,
        "vwap_pct": vwap_pct,
        "rsi": rsi,
    }, drop_reason


def run_execution_scan(service, regime="NEUTRAL", market_note=""):
    underdogs = build_underdog_list(service)
    rows = []
    drop_counts = {}

    spy_session = get_latest_rth_session("SPY", period="5d")
    spy_close = extract_col(spy_session, "Close")
    spy_day_chg = calc_pct_change(float(spy_close.iloc[-1]), float(spy_close.iloc[0])) if spy_close is not None and len(spy_close) > 1 else 0.0

    for t, bucket, score in underdogs:
        try:
            metrics, drop_reason = compute_intraday_metrics(t, spy_day_chg=spy_day_chg)
            if metrics is None:
                drop_counts[drop_reason] = drop_counts.get(drop_reason, 0) + 1
                continue

            status, sw, _ = classify_execution_status(
                regime,
                metrics["wk_chg"],
                metrics["rvol"],
                metrics["rs"],
                metrics["vwap_pct"],
                metrics["rsi"],
            )
            score_val = safe_float(score, 0.0)
            rank = calc_rank(sw, score_val, metrics["wk_chg"], metrics["rvol"], metrics["rs"], metrics["vwap_pct"], metrics["rsi"], status)
            rows.append((
                t, bucket, metrics["curr_p"], metrics["day_chg"], metrics["wk_chg"],
                score_val, metrics["rvol"], metrics["rs"], metrics["vwap_pct"], metrics["rsi"],
                status, rank
            ))
        except Exception as e:
            drop_counts["execution_exception"] = drop_counts.get("execution_exception", 0) + 1
            log_event("ERROR", "run_execution_scan", "candidate failed", ticker=t, error=str(e)[:160])

    rows.sort(key=lambda x: x[-1], reverse=True)
    rows = rows[:TOP_N]

    title = f"🎯 Execution Scan — UnderRadar | TOP {TOP_N} | Regime: {regime}"
    if market_note:
        title += f" | {market_note}"

    lines = [
        title,
        "************************** HOT STOCKS ************************",
        "Ticker | Type | Price | Day% | Wk% | Score | RVol | RS | VWAP% | RSI | St | Rank",
        "**************************************************************",
    ]

    if not rows:
        lines.append("None")
    else:
        for t, bucket, p, d, w, sc, rvol, rs, vwap, rsi, st, rk in rows:
            icon = status_icon(st)
            lines.append(
                f"{icon} {t:<5} | {bucket:<6} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | "
                f"{sc:>5.1f} | {rvol:>4.1f}x | {rs:>+4.1f} | {vwap:>+5.1f}% | {rsi:>3.0f} | {st:<3} | {rk:>5.2f}"
            )

    if drop_counts:
        lines.append("--------------------------------------------------------------")
        lines.append("Drops: " + ", ".join(f"{k}={v}" for k, v in sorted(drop_counts.items())))
    lines.append("*********************** GOOD LUCK *****************************")
    return "\n".join(lines)


# =========================================================
# 9. MAIN ORCHESTRATOR
# =========================================================
def main():
    validate_environment()
    service = get_drive_service()

    watchlist, drive_logs = build_dynamic_watchlist(service)
    dashboard = get_market_dashboard()
    dashboard += f"\n🔍 Diagnostics:\n{drive_logs}\n"

    portfolio = get_portfolio_performance(watchlist)
    regime, market_note = get_market_regime()
    execution_scan = run_execution_scan(service, regime=regime, market_note=market_note)

    if SHOW_DEBUG:
        dashboard += "\n🧪 Debug Summary:\n" + get_debug_summary(15) + "\n"

    send_msg(f"{dashboard}\n{portfolio}")
    send_msg(execution_scan)


if __name__ == "__main__":
    main()
