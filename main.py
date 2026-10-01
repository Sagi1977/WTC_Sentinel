import os
import time
import io
import re
import json
import requests
import pandas as pd
import numpy as np
import yfinance as yf
import pytz
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# =========================================================
# 1. CONFIG 
# =========================================================
TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", "")).strip()
BASE = f"https://api.telegram.org/bot{TOKEN}" if TOKEN else ""
TOP_N = 10
SHOW_DEBUG = str(os.environ.get("SHOW_DEBUG", "false")).lower() == "true"
DRIVE_PREFIXES = ["Golden_Plan_STOCKS"]  # ETF הוסר — מיקוד במניות בלבד
TELEGRAM_LOG_FOLDER_ID = "1wl4RspMhAG8DwITH4gN_UngMri3kAlsy"  # TELEGRAM WEB folder

# ✅ Sharpe/Sortino/Drawdown History (27/07/2026)
# ---------------------------------------------------------------
# הקמה חד-פעמית (בדיוק כמו שעשית ל-Daily Log):
#   1. צור Google Doc ריק חדש ב-Drive שלך (שם מוצע: "WTC_Perf_History")
#   2. מתוך ה-URL של הקובץ (docs.google.com/document/d/<FILE_ID>/edit)
#      העתק את ה-<FILE_ID> והדבק כאן במקום ה-placeholder
#   3. ודא שה-service account שלך (אותו אחד שכבר עובד ל-Daily Log) הוא
#      Editor על הקובץ הזה
# בלי זה — הקוד ירוץ בבטחה (try/except) אבל שורת ה-Sharpe/Sortino לא תופיע
PERF_HISTORY_FILE_ID = "1mnkis101utoLZ735XOSvWqxzDzZKUuIxzClz-Py-4tM"

# ✅ Signal Hysteresis History (29/07/2026)
# ---------------------------------------------------------------
# תיקון לבאג המתועד למטה ("Hysteresis הוסר 18/07/2026") — הפעם עם
# persistence אמיתי ל-Drive, לפי ריצת סוף-יום (23:15 ישראל) בלבד —
# לא לפי ספירת ריצות תוך-יומיות (זה בדיוק מה שנשבר בפעם הקודמת).
# הקמה: אותו תהליך בדיוק כמו PERF_HISTORY_FILE_ID למעלה — Google Doc
# ריק חדש, לשתף עם אותו service account, ולהדביק את ה-ID כאן.
SIGNAL_HISTORY_FILE_ID = "1c3uo8Okn0dBDyWfMDvl8UoL-N0Qdw-_eYwoEcrdjVHw"

# ✅ Exit-Risk Deterioration Baseline (26/09/2026)
# ---------------------------------------------------------------
# תיעוד ל-Drive של "הרגע הראשון היום" שבו מניה מוחזקת סומנה EXIT
# RISK/CAUTION — כדי ש-get_early_weakness_alerts תוכל להבחין בין
# החמרה שנמשכת בפועל לבין התאוששות (bounce). ראו ההערה המלאה ליד
# track_exit_deterioration למטה. הקמה: אותו תהליך כמו SIGNAL_HISTORY_FILE_ID
# למעלה — Google Doc ריק חדש, לשתף עם אותו service account, להדביק ID כאן.
EXIT_BASELINE_FILE_ID = "1tEGATqVMyOEEWlNM7PFAbgAxT0IH0nED19qBrYYFEbw"

SELECTION_PATTERN = r"Anchor|Turbo|Top 5"
RTH_TZ = "America/New_York"
RTH_START = (9, 30)
RTH_END_HOUR = 16

DATA_CACHE = {}
# SIGNAL_HISTORY הוסר — ראה הערה ב-build_underdog_list (Hysteresis bug)
DEBUG_EVENTS = []

# =========================================================
# 2. DEBUG / LOGGING
# =========================================================
def log_event(level, where, message, **kwargs):
    # 🔧 תוקן 29/07/2026 — ממצא קריטי: SHOW_DEBUG=False כברירת מחדל (אין
    # אותו מוגדר ב-workflow), כלומר log_event מעולם לא הדפיס כלום, גם על
    # שגיאות אמיתיות. זה הפך את כל ה-try/except החדשים ב-main() לשקטים
    # לגמרי — אי אפשר היה לדעת שמשהו נכשל, לא בלוג של GitHub Actions
    # ולא בשום מקום אחר. עכשיו: ERROR תמיד מודפס, בלי קשר ל-SHOW_DEBUG.
    payload = {"level": level, "where": where, "message": message}
    if kwargs:
        payload.update(kwargs)
    DEBUG_EVENTS.append(payload)
    if SHOW_DEBUG or level == "ERROR":
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
    # 🔧 תיקון 09/08/2026: google.auth.default() בלי scopes מפורש, עם
    # Workload Identity Federation (כמו ב-sentinel_run.yml), נוטה להחזיר
    # הרשאות עם scope כללי (cloud-platform) שלא תמיד כולל גישה מספקת
    # ל-Drive/Docs API (בפרט export_media, ש-log_to_drive משתמשת בו).
    # התוצאה בפועל: build() מצליח (לא זורק שגיאה), אבל קריאות ה-API
    # בפועל נכשלות עם 403/insufficient scope - בדיוק התסמין המדווח
    # ("הכל עובד חוץ מ-Daily Log"). הוספת ה-scope המפורש היא הפתרון
    # הישיר לתבנית הזו.
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds, _ = google.auth.default(scopes=SCOPES)
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
            return None, "❓ Missing", None

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]["id"]))
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding="utf-8-sig", engine="python")
        df = normalize_columns(df)
        validate_output_schema(df, prefix)
        return df, f"Loaded: {files[0]['name']}", files[0]['name']
    except Exception as e:
        log_event("ERROR", "download_latest_file", "failed to load output", prefix=prefix, error=str(e)[:250])
        return None, f"Err: {str(e)[:60]}", None


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
    # שימוש ב-pd.isna לזיהוי NaN לפני המרה
    if pd.isna(current) or pd.isna(base):
        return 0.0
    current = safe_float(current, float('nan'))
    base = safe_float(base, float('nan'))
    if pd.isna(current) or pd.isna(base) or base <= 0:
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
        if spy_close is not None:
            spy_close = spy_close.dropna()  # ניקוי NaN
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
        spy_2d = get_cached_yf_download("SPY", period="5d", interval="1d")
        spy_cls = extract_col(spy_2d, "Close")
        if spy_cls is not None:
            spy_cls = spy_cls.dropna()  # ניקוי NaN לפני iloc
        if spy_cls is None or len(spy_cls) < 2:
            return "📊 WTC Sentinel Dashboard\n------------------------------\n⚠️ Dashboard Offline\n"

        s_p = float(spy_cls.iloc[-1])
        prev_c = float(spy_cls.iloc[-2])
        s_c = calc_pct_change(s_p, prev_c)

        vix_hist = get_cached_yf_history("^VIX", period="5d")
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
    golden_file_dt = None
    for prefix in DRIVE_PREFIXES:
        df, status, fname = download_latest_file(service, prefix)
        if df is None:
            logs.append(f"❌ {prefix}: {status}")
            continue
        if fname and golden_file_dt is None:
            golden_file_dt = parse_golden_file_dt(fname)
        try:
            tcol, sel, scol = validate_output_schema(df, prefix)
            inv_col  = next((c for c in df.columns if "invest" in str(c).lower()), None)
            stop_col = next((c for c in df.columns if "stop" in str(c).lower()), None)  # ✅ Sprint 1
            mask = df[sel].astype(str).str.contains(SELECTION_PATTERN, na=False, case=False)
            for _, row in df[mask].iterrows():
                ticker = str(row[tcol]).strip().upper()
                if not ticker:
                    continue
                watchlist[ticker] = {
                    "label": str(row[sel]),
                    "score": row.get(scol, np.nan) if scol else np.nan,
                    "source": prefix,
                    "invest": safe_float(row.get(inv_col, np.nan), 0.0) if inv_col else 0.0,
                    "stop_loss": safe_float(row.get(stop_col, np.nan), 0.0) if stop_col else 0.0,  # ✅ Sprint 1
                }
            logs.append(f"✅ {prefix}: Found {int(mask.sum())}")
        except Exception as e:
            logs.append(f"⚠️ {prefix}: {str(e)[:60]}")
            log_event("ERROR", "build_dynamic_watchlist", "watchlist build failed", prefix=prefix, error=str(e)[:160])
    return watchlist, "\n".join(logs), golden_file_dt


# =========================================================
# ✅ ENTRY BASELINE — יישור קו: כניסה לפי timestamp של קובץ Golden
# הכלל: תאריך כניסה = יום המסחר הראשון שנפתח אחרי יצירת הקובץ
#        מחיר כניסה = מחיר הפתיחה (Open) של אותו יום
# =========================================================
import re as _re
from datetime import datetime as _dt, timedelta as _td


def parse_golden_file_dt(filename):
    """מחלץ datetime משם קובץ כמו Golden_Plan_STOCKS_20260608_202004.csv"""
    try:
        m = _re.search(r'(\d{8})_(\d{6})', str(filename))
        if not m:
            return None
        return _dt.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    except Exception:
        return None


def get_entry_baseline(ticker, file_dt):
    """
    מחזיר (entry_date, entry_open) — נקודת הכניסה לפי יישור הקו.
    הקובץ נוצר בשעון ישראל; ההמרה ל-ET נעשית עם pytz (DST-safe).
    אם הקובץ נוצר לפני פתיחת השוק (09:30 ET) → הכניסה באותו יום מסחר.
    אחרת → ביום המסחר הבא. מחזיר (None, None) אם יום הכניסה טרם נסחר.
    """
    if file_dt is None:
        return None, None
    try:
        # ✅ Sprint 1: המרה DST-safe עם pytz (במקום hours=7 קשיח)
        israel_tz = pytz.timezone('Asia/Jerusalem')
        et_tz     = pytz.timezone('America/New_York')
        file_localized = israel_tz.localize(file_dt) if file_dt.tzinfo is None else file_dt
        file_et = file_localized.astimezone(et_tz)

        target_date = file_et.date()
        if file_et.hour > 9 or (file_et.hour == 9 and file_et.minute >= 30):
            target_date = target_date + _td(days=1)

        daily = get_cached_yf_download(ticker, period="1mo", interval="1d")
        open_s = extract_col(daily, "Open")
        if open_s is None:
            return None, None
        open_s = open_s.dropna()
        if open_s.empty:
            return None, None
        for idx, val in open_s.items():
            d = idx.date() if hasattr(idx, "date") else pd.to_datetime(idx).date()
            if d >= target_date:
                return d, safe_float(val, None)
        return None, None  # יום הכניסה עוד לא נסחר (למשל מריצים בלילה לפני)
    except Exception as e:
        log_event("ERROR", "get_entry_baseline", "entry baseline failed", ticker=ticker, error=str(e)[:160])
        return None, None


def log_to_drive(service, message):
    """
    שומר כל הודעת טלגרם על ידי עדכון קובץ Google Doc קיים ב-Drive.
    משתמש ב-File ID ספציפי כדי לעקוף את בעיית יצירת הקבצים של חשבונות שירות.
    זמן הלוג מומר לשעון ישראל (Jerusalem).

    🔧 (26/09/2026) — ממצא: עד עכשיו, אם service היה None, הפונקציה הזו
    פשוט עשתה return בשקט - בלי אף log_event, בלי שום עקבה. כלומר אם
    האותנטיקציה ל-Drive נכשלת (בדיוק התבנית המתועדת ב-get_drive_service:
    build() מצליח אבל קריאות בפועל נכשלות), לא היה שום סימן שהלוג בכלל
    לא נכתב - לא בקונסול, לא בטלגרם. עכשיו: תמיד log_event, ותמיד
    מחזירה True/False כדי שההרצה שקוראת לה תדע אם לדווח על כשל בטלגרם
    (הערוץ שבאמת עוקבים אחריו) ולא רק בלוג של GitHub Actions שאף אחד
    לא פותח בזמן אמת.
    """
    if service is None:
        log_event("ERROR", "log_to_drive", "Drive service is None - log NOT written this run")
        return False

    from datetime import datetime as _dt2
    import pytz
    import io
    import random
    from googleapiclient.http import MediaIoBaseUpload

    # הגדרת אזור זמן ישראל
    il_tz = pytz.timezone('Asia/Jerusalem')

    # לקיחת הזמן משרת ה-UTC והמרתו לישראל
    now_utc = _dt2.now(pytz.utc)
    now_il = now_utc.astimezone(il_tz)

    ts = now_il.strftime("%Y-%m-%d %H:%M:%S")
    entry = f"\n{'='*55}\n[{ts}]\n{message}\n"

    # ה-ID המדויק של הקובץ שיצרת
    # 🔧 תוקן 26/09/2026 — ממצא שורש-הבעיה: ה-FILE_ID הישן
    # (1H0XnJrj7mwQK_7dLtCFw6wxWMK61IchobvPFP4c-xlI, מ-30/07/2026)
    # מתייחס למסמך שאינו נגיש בכלל לחשבון ה-Google האישי של המשתמש
    # (sagi.taragan@gmail.com) - כנראה נוצר/שותף רק מול ה-service
    # account. הקוד כתב אליו בהצלחה כל הזמן (זו הסיבה שההרצות דיווחו
    # "[CONFIRM] הצליח") - אבל לתוך מסמך שהמשתמש מעולם לא ראה. בינתיים
    # המשתמש פתח/יצר מסמך אחר בשם "Daily Log" (בתיקיית TELEGRAM WEB)
    # וזה שנשאר ריק. אומת ישירות מול Drive: המסמך הבא כבר משותף
    # ל-service account (sentinel-handler@project-c833aec6-507e-4e48-bb7
    # .iam.gserviceaccount.com) בהרשאת writer - כך שאין צורך בשום שיתוף
    # נוסף, רק בעדכון ה-ID הזה.
    FILE_ID = '1FZfO8R6OaklSsDb0fTQV-f-MpnP1mxIdsCC_ciXFycw'  # "Daily Log" האמיתי שהמשתמש רואה

    # 🔧 (29/09/2026) #16: לפני התיקון, כל קריאה ל-log_to_drive עשתה
    # read-modify-write "עיוור" בלי שום בדיקה שהתוכן לא השתנה בין הקריאה
    # לכתיבה: קוראת (export_media) → מוסיפה שורה בזיכרון → כותבת חזרה
    # (files().update) על בסיס מה שקראה. אם שתי הרצות רצות במקביל (למשל
    # workflow_dispatch ידני שחפף לריצת cron מתוזמנת) קוראות את אותו
    # תוכן לפני ששתיהן כתבו - השנייה שכותבת דורסת בשקט את מה שהראשונה
    # כתבה: אין שגיאה, אין לוג, ההודעה של ההרצה הראשונה נעלמת מהדוח.
    #
    # Drive API v3 לא חושף CAS אמיתי (if-match/generation precondition)
    # לעדכון Google Docs, כמו שיש למשל ל-GCS objects - אז אי אפשר לחסום
    # את התחרות ב-100% מכאן בלבד (התיקון האמיתי והמלא הוא להוסיף
    # `concurrency:` group בקובץ ה-workflow של GitHub Actions, כדי
    # שהרצות חופפות לא יתבצעו כלל - זה מחוץ ל-main.py). מה שאפשר לעשות
    # בקוד: בדיקת concurrency אופטימית באמצעות headRevisionId - קוראים
    # אותו לפני בניית התוכן החדש ושוב ממש לפני הכתיבה; אם הוא השתנה
    # בין השתיים, מישהו אחר כתב באמצע → לא כותבים על בסיס תוכן שכבר לא
    # עדכני, קוראים הכל מחדש ומנסים שוב (עד MAX_RETRIES, עם sleep+jitter
    # קטן כדי לפזר הרצות שמתנגשות). זה מצמצם דרסטית את חלון-התחרות
    # ומהפך אובדן-נתונים שקט למקרה נדיר שגם מתועד ב-log_event כ-WARNING
    # אם הוא בכלל קורה - לא מבטל את הסיכון התיאורטי לב-100%, אבל
    # במקום "תמיד דורס בשקט בלי עדות" מקבלים "עדות + ניסיון חזרה".
    MAX_RETRIES = 5
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            meta_before = service.files().get(fileId=FILE_ID, fields='headRevisionId').execute()
            rev_before = meta_before.get('headRevisionId')

            # משיכת התוכן הקיים מהקובץ
            res = service.files().export_media(fileId=FILE_ID, mimeType='text/plain').execute()
            existing_content = res.decode('utf-8', errors='replace')

            # --- מנגנון ניקוי אוטומטי ---
            # פירוק הטקסט לשורות ושמירת 76,000 השורות האחרונות בלבד (מונע מהקובץ להתנפח)
            # 🔧 תיקון 02/09/2026: הרף הישן (1000 שורות) התמלא כמעט בכל הרצה
            # בודדת - ההיסטוריה נמחקה תוך שעות, לא ימים. הועלה ל-76,000 שורות
            # (~950K תווים) - קרוב למקסימום הבטוח של Google Docs (~1.02M
            # תווים), נותן כ-3.2 ימי היסטוריה בפועל. זו התקרה הפיזית של הגישה
            # הזו (מסמך יחיד) - לא ניתן להגיע ל-4 ימים מלאים בלי לצמצם את
            # אורך כל רשומה בנפרד, לא רק את מספר השורות הנשמרות.
            # 🔧 (01/10/2026) #37 — שורש הבעיה הוכח: כל סבב export(text/plain)→
            # update(text/plain→Google Doc) מכפיל פי 2 כל שורה ריקה קיימת. אחרי
            # כמה ימים: 151,779 שורות, רק 242 עם טקסט. התוצאה: (1) כל כתיבה מעלה
            # ~150K פסקאות ריקות → Google מחזיר HttpError 500; (2) חיתוך ה-76,000
            # שורות מחק בפועל כמעט את כל ההיסטוריה (~7 ריצות נשמרו). מכווצים כל
            # רצף של 2+ שורות ריקות לשורה ריקה אחת לפני כל כתיבה — גם מרפא את
            # הקובץ הקיים בכתיבה הראשונה.
            existing_content = re.sub(r'\n[ \t\u00a0\ufeff]*(?:\n[ \t\u00a0\ufeff]*)+\n', '\n\n', existing_content)

            lines = existing_content.split('\n')
            if len(lines) > 76000:
                lines = lines[-76000:]  # חותך את ההיסטוריה הישנה
                existing_content = "=== [LOG TRUNCATED - OLD DATA REMOVED] ===\n" + '\n'.join(lines)

            # חיבור התוכן החדש לישן
            new_content = existing_content + entry

            # בדיקה אופטימית שנייה, ממש לפני הכתיבה - מצמצמת את חלון התחרות
            meta_after = service.files().get(fileId=FILE_ID, fields='headRevisionId').execute()
            rev_after = meta_after.get('headRevisionId')
            if rev_before is not None and rev_after is not None and rev_before != rev_after:
                log_event("WARNING", "log_to_drive",
                          f"Detected concurrent Drive write (headRevisionId {rev_before} -> {rev_after}) "
                          f"- re-reading and retrying ({attempt}/{MAX_RETRIES}) instead of overwriting")
                time.sleep(0.5 + random.uniform(0, 1.5))
                continue

            # עדכון הקובץ ב-Drive
            media = MediaIoBaseUpload(
                io.BytesIO(new_content.encode('utf-8')),
                mimetype='text/plain'
            )
            service.files().update(
                fileId=FILE_ID,
                media_body=media
            ).execute()

            log_event("INFO", "log_to_drive", "Successfully updated Drive log file (IL Time).")
            # 🔧 אבחון זמני (29/07/2026) — הדפסה שתמיד מופיעה, בלי תלות ב-SHOW_DEBUG,
            # כדי לדעת בוודאות אם הכתיבה הצליחה ולאיזה FILE_ID בדיוק
            print(f"[CONFIRM] log_to_drive: כתיבה הצליחה ל-FILE_ID={FILE_ID} | אורך תוכן חדש={len(new_content)} תווים")
            return True
        except Exception as e:
            # 🔧 (01/10/2026) — שגיאת שרת זמנית של Google (5xx/429) מקבלת ניסיון
            # חוזר עם המתנה, במקום כשל מיידי. שגיאה אחרת (הרשאות וכו') — כשל מיידי.
            status = getattr(getattr(e, 'resp', None), 'status', None)
            try:
                status = int(status) if status is not None else None
            except (TypeError, ValueError):
                status = None
            if status in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                log_event("WARNING", "log_to_drive",
                          f"Drive returned HTTP {status} - retrying ({attempt}/{MAX_RETRIES})")
                time.sleep(2 * attempt + random.uniform(0, 1.5))
                continue
            log_event("ERROR", "log_to_drive", "telegram log failed", error=str(e)[:120])
            return False

    log_event("ERROR", "log_to_drive",
              f"Gave up after {MAX_RETRIES} retries due to repeated concurrent Drive writes - log NOT written this run")
    return False


# =========================================================
# ✅ Performance History — Sharpe / Sortino / Max Drawdown (27/07/2026)
# ---------------------------------------------------------------
# עקרון: כל שבוע (מזוהה לפי golden_file_dt) מקבל שורה אחת בקובץ CSV
# ששמור ב-Drive. כל הרצה (כמה פעמים ביום) מעדכנת (UPSERT) את השורה של
# השבוע הנוכחי עם ה-Alpha העדכני ביותר — כך שבסוף השבוע, כשמופיע Golden
# Plan חדש, השורה הישנה כבר "קפאה" על הערך האחרון שלה. אין צורך לזהות
# "סוף שבוע" באופן מיוחד.
# חשוב: זה קורא/כותב ל-PERF_HISTORY_FILE_ID בלבד — קובץ נפרד לגמרי
# מה-Daily Log (TELEGRAM), כדי לא לסכן קטיעה של הלוג הקיים ל-1000 שורות
# (נתוני ביצועים לא אמורים להיחתך לעולם).
# =========================================================

def _perf_sharpe(returns_pct, periods_per_year=52):
    """גרסה קומפקטית (ללא numpy) של Sharpe Ratio - עקבית עם performance_metrics.py"""
    n = len(returns_pct)
    if n < 2:
        return None
    r = [x / 100.0 for x in returns_pct]
    mean = sum(r) / n
    var = sum((x - mean) ** 2 for x in r) / (n - 1)
    std = var ** 0.5
    if std == 0:
        return None
    return (mean / std) * (periods_per_year ** 0.5)


def _perf_sortino(returns_pct, periods_per_year=52):
    """גרסה קומפקטית של Sortino Ratio - עקבית עם performance_metrics.py"""
    n = len(returns_pct)
    if n < 2:
        return None
    r = [x / 100.0 for x in returns_pct]
    mean = sum(r) / n
    downside = [x for x in r if x < 0]
    if not downside:
        return None
    downside_dev = (sum(x ** 2 for x in downside) / len(downside)) ** 0.5
    if downside_dev == 0:
        return None
    return (mean / downside_dev) * (periods_per_year ** 0.5)


def _perf_max_drawdown(returns_pct):
    """גרסה קומפקטית של Max Drawdown - עקבית עם performance_metrics.py"""
    equity, peak, max_dd = 1.0, 1.0, 0.0
    for x in returns_pct:
        equity *= (1 + x / 100.0)
        peak = max(peak, equity)
        dd = (equity - peak) / peak
        max_dd = min(max_dd, dd)
    return max_dd * 100


def get_perf_history(service):
    """
    קורא את היסטוריית הביצועים מ-Drive. מחזיר list of dicts:
    [{'week_key': '2026-07-21', 'portfolio_return_pct': -5.9, 'qqq_return_pct': -3.2, 'alpha_pct': -2.8}, ...]
    מסודר כרונולוגית. מחזיר [] בכל כשל (שקט, לא עוצר את הריצה).
    """
    if service is None or not PERF_HISTORY_FILE_ID or "PASTE_YOUR" in PERF_HISTORY_FILE_ID:
        return []
    try:
        res = service.files().export_media(fileId=PERF_HISTORY_FILE_ID, mimeType='text/plain').execute()
        # 🔧 תוקן 29/07/2026: decode עם utf-8-sig מסיר BOM בתחילת הקובץ,
        # אבל Google Docs מכניס גם תווים בלתי-נראים בתחילת *כל שורה*
        # (לא רק שורה ראשונה) — לכן מנקים כל שורה בנפרד למטה.
        content = res.decode('utf-8-sig', errors='replace').strip()
        if not content:
            return []
        rows = []
        for line in content.split('\n'):
            # 🔧 מסירים כל תו לא-נראה/BOM מתחילת השורה (\ufeff וכדומה),
            # לא רק רווחים רגילים — .strip() לבד לא תופס את זה
            clean_line = line.lstrip('\ufeff\u200b\u200c\u200d ').strip()
            parts = clean_line.split(',')
            if len(parts) != 4:
                continue
            try:
                # ניקוי דומה גם על ה-week_key עצמו (הגנה כפולה)
                week_key_clean = re.sub(r'[^\d\-]', '', parts[0])
                rows.append({
                    'week_key': week_key_clean,
                    'portfolio_return_pct': float(parts[1]),
                    'qqq_return_pct': float(parts[2]),
                    'alpha_pct': float(parts[3]),
                })
            except ValueError:
                continue

        # 🔧 תוקן 29/07/2026: ניקוי כפילויות — שומר את המופע האחרון לכל
        # week_key. זה מרפא בעצמו קבצים שכבר הצטברו בהם כפילויות (כמו
        # הבאג עם ה-BOM שגרם ל-12 שורות לאותו תאריך) — בהרצה הבאה, הכתיבה
        # החוזרת תשמור רק שורה אחת נקייה לכל שבוע.
        deduped = {}
        for row in rows:
            deduped[row['week_key']] = row  # מופע מאוחר יותר דורס מוקדם
        rows = list(deduped.values())

        rows.sort(key=lambda r: r['week_key'])
        return rows
    except Exception as e:
        log_event("WARN", "get_perf_history", "read failed - returning empty history", error=str(e)[:120])
        return []


def upsert_perf_history(service, week_key, port_ret, qqq_ret, alpha):
    """
    מעדכן (או מוסיף) את השורה של week_key עם הערכים העדכניים ביותר,
    ושומר בחזרה ל-Drive. לא זורק שגיאה החוצה - כשל כאן לא אמור לעצור
    את שאר הדוח.
    """
    if service is None or not PERF_HISTORY_FILE_ID or "PASTE_YOUR" in PERF_HISTORY_FILE_ID:
        return
    try:
        history = get_perf_history(service)
        found = False
        for row in history:
            if row['week_key'] == week_key:
                row['portfolio_return_pct'] = round(port_ret, 3)
                row['qqq_return_pct'] = round(qqq_ret, 3)
                row['alpha_pct'] = round(alpha, 3)
                found = True
                break
        if not found:
            history.append({
                'week_key': week_key,
                'portfolio_return_pct': round(port_ret, 3),
                'qqq_return_pct': round(qqq_ret, 3),
                'alpha_pct': round(alpha, 3),
            })
        history.sort(key=lambda r: r['week_key'])

        content = '\n'.join(
            f"{r['week_key']},{r['portfolio_return_pct']},{r['qqq_return_pct']},{r['alpha_pct']}"
            for r in history
        )
        media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain')
        service.files().update(fileId=PERF_HISTORY_FILE_ID, media_body=media).execute()
    except Exception as e:
        log_event("WARN", "upsert_perf_history", "write failed - history not updated this run", error=str(e)[:120])


def format_perf_summary(history, min_weeks_for_confidence=20):
    """
    בונה את שורת הדוח עם Sharpe/Sortino/MaxDD. מחזיר מחרוזת ריקה אם אין
    מספיק נתונים (n<2), או אם משהו נכשל בחישוב עצמו — במקרה כזה זו
    מחרוזת ריקה, לא חריגה שתופיע החוצה.
    """
    try:
        n = len(history)
        if n < 2:
            return ""

        alpha_series = [r['alpha_pct'] for r in history]
        port_series = [r['portfolio_return_pct'] for r in history]

        sharpe_a = _perf_sharpe(alpha_series)
        sortino_a = _perf_sortino(alpha_series)
        mdd_p = _perf_max_drawdown(port_series)

        sharpe_str = f"{sharpe_a:.2f}" if sharpe_a is not None else "N/A"
        sortino_str = f"{sortino_a:.2f}" if sortino_a is not None else "N/A"

        lines = [f"📐 Perf History ({n} שבועות): Sharpe(α)={sharpe_str} | Sortino(α)={sortino_str} | MaxDD(Portfolio)={mdd_p:+.1f}%"]
        if n < min_weeks_for_confidence:
            lines.append(f"   ⚠️ מדגם קטן ({n}/{min_weeks_for_confidence}+) — אינדיקציה ראשונית, לא מסקנה סטטיסטית")
        return '\n'.join(lines)
    except Exception:
        return ""  # כשל בחישוב = פשוט לא מציגים את השורה, לא מפילים את הדוח


# =========================================================
# ✅ Signal Hysteresis — אישור BUY על פני ≥2 ימי מסחר (29/07/2026)
# ---------------------------------------------------------------
# עקרון: בניגוד לניסיון הקודם (שספר "ריצות רצופות" בזיכרון, ונשבר כי
# GitHub Actions הוא תהליך חדש בכל פעם) — כאן שומרים רק *ריצת סוף-יום
# אחת* לכל מניה לכל יום מסחר, ב-Drive. סיגנל BUY מוצג כ"מאושר" רק אם
# גם ביום המסחר הקודם שנשמר הציון היה ≥5 (BUY-worthy). זה תואם את מה
# שנפוץ בעולם המסחר: אישור על פני timeframe ארוך יותר (יום מול יום),
# לא עוד ועוד בדיקות תוך-יומיות (שרועשות מטבען, למשל RVol).
# =========================================================

def is_end_of_day_run():
    """
    בודק אם ההרצה הנוכחית היא ריצת 'סוף היום' (16:15 ET — כ-23:15 ישראל
    ברוב השנה, לפי sentinel_run.yml + market_gate.py) — רק ריצה כזו
    כותבת ל-Signal Hysteresis History.

    🔧 (29/09/2026, NEW2 — ראו wtc-monday-timetable.md) — הגרסה הקודמת
    בדקה רק "שעת ישראל == 23" (hour בלבד, בלי דקות). ממצאים: ריצה
    שהתעכבה 30+ דקות (22:30 בפועל, GitHub Actions לפעמים מעכב בעומס)
    לא נספרה כ-EOD בזמן; ריצה שהתעכבה אחרי חצות לא נספרה בכלל; בשבועות
    שבהם ישראל וארה"ב עוברות שעון בתאריכים שונים ה-EOD בפועל נופל
    ב-22:15, לא ב-23:xx. עכשיו: מסתמכת קודם כל על WTC_RUN_KIND
    (env var) — מגיע מ-market_gate.py דרך sentinel_run.yml, שכבר עשה
    את כל חשבון ה-DST הנכון לפי שעון ניו יורק (הזמן *המתוכנן* של
    הטריגר, לא זמן-ריצה בפועל, כך שעיכוב לא משנה את הסיווג). אם
    המשתנה חסר או לא מזוהה (למשל הרצה ישנה/ידנית לפני חיבור ה-workflow
    לגייט) — נופלת לגיבוי המתועד: שעון ניו יורק >= 16:00.
    """
    run_kind = os.environ.get('WTC_RUN_KIND', '').strip().upper()
    if run_kind in ('EOD', 'INTRADAY'):
        return run_kind == 'EOD'
    try:
        from datetime import datetime as _dt3, time as _dtime3
        ny_tz = pytz.timezone('America/New_York')
        return _dt3.now(ny_tz).time() >= _dtime3(16, 0)
    except Exception:
        return False


def get_signal_history(service):
    """
    קורא היסטוריית סיגנלים מ-Drive. מחזיר dict:
    {ticker: [(date_str, score), ...]} ממוין כרונולוגית לכל מניה.
    מחזיר {} בכל כשל (שקט, לא עוצר את הריצה).
    """
    if service is None or not SIGNAL_HISTORY_FILE_ID or "PASTE_YOUR" in SIGNAL_HISTORY_FILE_ID:
        return {}
    try:
        res = service.files().export_media(fileId=SIGNAL_HISTORY_FILE_ID, mimeType='text/plain').execute()
        content = res.decode('utf-8-sig', errors='replace').strip()
        if not content:
            return {}
        history = {}
        for line in content.split('\n'):
            clean_line = line.lstrip('\ufeff\u200b\u200c\u200d ').strip()
            parts = clean_line.split(',')
            if len(parts) != 3:
                continue
            try:
                ticker = parts[0].strip()
                date_str = re.sub(r'[^\d\-]', '', parts[1])
                score = float(parts[2])
                history.setdefault(ticker, {})[date_str] = score  # dict = דה-דופ אוטומטי
            except ValueError:
                continue
        return {t: sorted(d.items()) for t, d in history.items()}
    except Exception as e:
        log_event("WARN", "get_signal_history", "read failed - returning empty", error=str(e)[:120])
        return {}


def upsert_signal_history(service, updates, max_days_per_ticker=5):
    """
    updates: dict {ticker: (date_str, score)} — עדכון סוף-יום אחד לכל
    מניה. שומר רק max_days_per_ticker הימים האחרונים לכל מניה כדי
    שהקובץ לא יתנפח עם השבועות.
    """
    if service is None or not SIGNAL_HISTORY_FILE_ID or "PASTE_YOUR" in SIGNAL_HISTORY_FILE_ID:
        return
    try:
        history = get_signal_history(service)
        for ticker, (date_str, score) in updates.items():
            existing = dict(history.get(ticker, []))
            existing[date_str] = round(float(score), 2)
            history[ticker] = sorted(existing.items())[-max_days_per_ticker:]

        lines = []
        for ticker, days in history.items():
            for date_str, score in days:
                lines.append(f"{ticker},{date_str},{score}")
        content = '\n'.join(lines)
        media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain')
        service.files().update(fileId=SIGNAL_HISTORY_FILE_ID, media_body=media).execute()
    except Exception as e:
        log_event("WARN", "upsert_signal_history", "write failed - history not updated", error=str(e)[:120])


def confirm_buy_signal(signal, sig_score, ticker, history, today_str, buy_threshold=5, required_days=2):
    """
    משדרג BUY ל'מאושר' רק אם גם ביום המסחר הקודם שנשמר הציון היה
    ≥buy_threshold. לא נוגע ב-WEAK/WAIT/AVOID — רק ב-BUY.
    מחזיר: (signal_להצגה, is_confirmed: bool)
    """
    if signal != "🟢 BUY":
        return signal, False
    days = [d for d in history.get(ticker, []) if d[0] != today_str]  # לא כולל היום עצמו
    if len(days) < required_days - 1:
        return "🟡 PENDING (Day 1/2)", False
    last_date, last_score = days[-1]
    if last_score >= buy_threshold:
        return "🟢 BUY", True
    return "🟡 PENDING (Day 1/2)", False


def classify_portfolio_status(day_chg, wk_chg, pnl=None, curr_price=None, stop_loss=None):
    """
    ✅ Sprint 1: סטטוס entry-relative.
    עדיפות 1: Stop Loss חצוי → ❌ Bel (יציאה!)
    עדיפות 2: P&L מהכניסה (אם קיים) קובע את הסטטוס
    fallback:  ההיגיון הישן (wk_chg/day_chg) אם אין entry data
    """
    # עדיפות 1 — Stop Loss חצוי = יציאה מיידית, בלי קשר לשום דבר אחר
    if stop_loss is not None and curr_price is not None and stop_loss > 0 and curr_price < stop_loss:
        return "❌ Bel", "BELOW STOP LOSS — EXIT NOW"

    # עדיפות 2 — סטטוס לפי P&L אמיתי מהכניסה
    if pnl is not None:
        if pnl >= 8 and day_chg >= 0:
            return "✅ Str", "Strong gain from entry"
        if pnl >= 3:
            return "👀 Bld", "Building from entry"
        if pnl >= -1.5:
            return "🟦 Hold", "Holding near entry"
        if pnl >= -4:
            return "⚠️ Weak", "Losing from entry"
        return "❌ Bel", "Deep loss from entry"

    # fallback — ההיגיון הישן (למקרה שאין entry baseline)
    if wk_chg >= 8 and day_chg >= 1:
        return "✅ Str", "Strong weekly and daily action"
    if wk_chg >= 3 and day_chg >= 0:
        return "👀 Bld", "Building constructively"
    if -0.5 <= wk_chg < 3 and day_chg > -1.0:
        return "🟦 Hold", "Holding near weekly base"
    if wk_chg >= 0 or day_chg > -2:
        return "⚠️ Weak", "Weak momentum / stalling"
    return "❌ Bel", "Below acceptable strength"


def get_portfolio_performance(watchlist, golden_file_dt=None, service=None):
    if not watchlist:
        return "📈 My Portfolio Watch (Dynamic)\n------------------------------\n⚠️ Watchlist empty\n"

    report = []
    report.append("📈 My Portfolio Watch (Dynamic)")
    if golden_file_dt is not None:
        report.append(f"Entry baseline: open of first session after {golden_file_dt.strftime('%d/%m %H:%M')}")
    report.append("-" * 64)
    report.append("Type | Ticker | Entry | Price | Day% | Wk% | P&L% | vsQQQ | Status")
    report.append("-" * 64)

    # QQQ baseline פעם אחת — אותה נקודת כניסה לכל הפוזיציות
    qqq_entry_date, qqq_entry_open = get_entry_baseline("QQQ", golden_file_dt)
    qqq_curr = None
    try:
        qqq_session = get_latest_rth_session("QQQ", period="5d")
        qqq_close_s = extract_col(qqq_session, "Close")
        if qqq_close_s is not None and not qqq_close_s.empty:
            qqq_curr = float(qqq_close_s.iloc[-1])
    except Exception:
        qqq_curr = None
    qqq_ret = None
    if qqq_entry_open and qqq_curr:
        qqq_ret = calc_pct_change(qqq_curr, qqq_entry_open)

    total_invest, total_pnl_weighted = 0.0, 0.0

    for t, info in watchlist.items():
        try:
            session_df = get_latest_rth_session(t, period="5d")
            close_s = extract_col(session_df, "Close")
            open_s = extract_col(session_df, "Open")
            if session_df is None or session_df.empty or close_s is None or close_s.empty or open_s is None or open_s.empty:
                report.append(f"{'N/D':<9} | {t:<6} | {'—':>6} | {'N/A':>6} | {'N/A':>5} | {'N/A':>5} | {'—':>5} | {'—':>5} | ⚠️")
                log_event("WARN", "get_portfolio_performance", "missing intraday data", ticker=t)
                continue

            curr_p = float(close_s.iloc[-1])
            day_open = float(open_s.iloc[0])

            # Day% נכון: שינוי מסגירת אתמול (כמו ברוקרים), לא מפתיחת היום
            prev_close_df = get_cached_yf_download(t, period="5d", interval="1d")
            prev_close_s = extract_col(prev_close_df, "Close")
            if prev_close_s is not None:
                prev_close_s = prev_close_s.dropna()
            # ה-iloc[-1] הוא היום הנוכחי, iloc[-2] הוא סגירת אתמול האמיתית
            prev_p = float(prev_close_s.iloc[-2]) if prev_close_s is not None and len(prev_close_s) >= 2 else day_open
            day_chg = calc_pct_change(curr_p, prev_p)   # מסגירת אתמול ✅

            wk_open = get_week_start_open(t)
            if wk_open is None:
                wk_open = prev_p
            wk_chg = calc_pct_change(curr_p, wk_open)

            # ✅ P&L מאז הכניסה (יישור קו: פתיחת היום שאחרי קובץ Golden)
            pnl_str, vsqqq_str = "  —  ", "  —  "
            pnl_val = None  # ✅ Sprint 1: entry-relative P&L ל-status
            _, entry_open = get_entry_baseline(t, golden_file_dt)
            if entry_open and entry_open > 0:
                pnl = calc_pct_change(curr_p, entry_open)
                pnl_val = pnl
                pnl_str = f"{pnl:>+5.1f}"
                if qqq_ret is not None:
                    vsqqq_str = f"{(pnl - qqq_ret):>+5.1f}"
                inv = safe_float(info.get("invest", 0.0), 0.0)
                if inv <= 0:
                    inv = 1.0  # fallback: שקלול שווה אם אין Invest_USD
                total_invest += inv
                total_pnl_weighted += pnl * inv

            # ✅ Sprint 1: סטטוס entry-relative + Stop Loss check
            sl = safe_float(info.get("stop_loss", 0.0), 0.0)
            status, _ = classify_portfolio_status(
                day_chg, wk_chg,
                pnl=pnl_val,
                curr_price=curr_p,
                stop_loss=sl if sl > 0 else None
            )
            lbl = str(info.get("label", "")).strip()
            lbl = (lbl[:7] + ".") if len(lbl) > 8 else lbl[:8]
            entry_str = f"{entry_open:>6.2f}" if entry_open and entry_open > 0 else f"{'—':>6}"
            report.append(
                f"{lbl:<9} | {t:<6} | {entry_str} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {pnl_str}% | {vsqqq_str}% | {status}"
            )
        except Exception as e:
            report.append(f"{'Err':<9} | {t:<6} | {'—':>6} | {'N/A':>6} | {'N/A':>5} | {'N/A':>5} | {'—':>5} | {'—':>5} | ❌")
            log_event("ERROR", "get_portfolio_performance", "portfolio row failed", ticker=t, error=str(e)[:160])

    report.append("-" * 64)

    # ✅ שורת Alpha — התיק מול QQQ מאותה נקודת כניסה
    if total_invest > 0 and qqq_ret is not None:
        port_ret = total_pnl_weighted / total_invest
        alpha = port_ret - qqq_ret
        icon = "✅" if alpha >= 0 else "🔻"
        report.append(f"📊 Portfolio: {port_ret:+.1f}% | QQQ: {qqq_ret:+.1f}% | Alpha: {alpha:+.1f}% {icon}")

        # ✅ Perf History (27/07/2026) — תוקן 28/07/2026: עכשיו הכל בתוך try/except
        # אחד ברמת הקריאה עצמה. לפני התיקון, רק upsert/get היו מוגנים
        # פנימית — אבל format_perf_summary וההרכבה עצמה לא, ותקלה שם
        # הייתה יכולה להפיל את כל הדוח (כולל השליחה לטלגרם והלוג!).
        try:
            if golden_file_dt is not None:
                week_key = golden_file_dt.strftime('%Y-%m-%d')
                upsert_perf_history(service, week_key, port_ret, qqq_ret, alpha)
                history = get_perf_history(service)
                perf_line = format_perf_summary(history)
                if perf_line:
                    report.append(perf_line)
        except Exception as e:
            log_event("ERROR", "get_portfolio_performance", "perf history block failed - report continues", error=str(e)[:160])
    elif golden_file_dt is not None and qqq_entry_open is None:
        report.append("📊 Alpha: — (יום הכניסה טרם נסחר)")

    return "\n".join(report) + "\n"


# =========================================================
# 🆕 Early Weakness Alerts — היחלשות אמיתית, לא רק Stop Loss (26/09/2026)
# ---------------------------------------------------------------
# רקע: השוואת שתי הרצות Golden Plan מ-21/09 (09:27 ET מול 14:03 ET)
# הראתה שמניה יכולה להיחלש באמת (RS מתמוטט מול SPY/QQQ, נפח נעלם,
# יורדת מתחת ל-MA200) הרבה לפני שהמחיר בפועל שובר את ה-Stop Loss שלה.
# classify_portfolio_status() למעלה כבר בודק Stop Loss (מחיר) ו-P&L
# (מחיר גם הוא) - שניהם מגיבים רק *אחרי* שהנזק כבר נראה במחיר.
#
# 🔧 (26/09/2026) — גרסה ראשונה השתמשה ב-confidence_signal() הקיים
# (RS/RVol/VWAP/RSI/MA200/52wHigh) כמו שהוא, בהנחה שאותה לוגיקה שכבר
# רצה על ה-underdogs מתאימה גם כאן. בבדיקה מול תרחיש AMP (-10.40%
# באותו שבוע) התגלו שני באגים אמיתיים: (1) compute_intraday_metrics()
# מכיל שער wk_chg>=5 שהיה פעיל ללא תנאי - מניה יורדת הייתה מדולגת
# בשקט לפני שהיא מגיעה לניקוד בכלל (0% סיכוי להתראה). (2)
# confidence_signal() עצמו בנוי למטרה ההפוכה - דירוג מועמד-*קנייה*
# חדש (New Buy Candidate) - ולא מעניש ירידה שבועית, מסחר מתחת ל-VWAP,
# או נפח-מכירה. שני הבאגים תוקנו: פרמטר require_min_weekly_change
# (False כאן, True ב-run_execution_scan - ראו שם) + exit_risk_signal()
# ייעודי, שכל גורם בו נבדק במפורש בכיוון הסיכון-לרדת (ראו התיעוד המלא
# בהגדרתו). confidence_signal() נשאר ללא שינוי ב-run_execution_scan,
# ששם ההיגיון המקורי שלו (מועמד-קנייה חדש) עדיין נכון.
# =========================================================
# =========================================================
# ✅ Exit-Risk Deterioration Confirmation — מבוסס אינדיקטורים, לא מונה-ריצות (26/09/2026)
# ---------------------------------------------------------------
# רקע: backtest אמיתי (26/09, 720 תצפיות היסטוריות) הראה שמניה
# שמסומנת EXIT RISK/CAUTION הרבה פעמים *מתאוששת* בהמשך (oversold
# bounce) ולא ממשיכה ליפול. לכן "לחכות ל-N ריצות רצופות עם אותה
# התראה" הוא הקריטריון הלא-נכון — מספר-ריצות הוא שרירותי ולא קשור
# לשאלה האמיתית: האם ההתדרדרות הטכנית נמשכת בפועל.
#
# העיקרון: כשמניה נכנסת ל-EXIT RISK/CAUTION בפעם הראשונה באותו יום
# מסחר, שומרים "בייסליין" — RSI/מחיר/VWAP%/ציון של אותו רגע — ל-Drive
# (main.py הוא stateless בין ריצות, כמו שכבר נלמד ב-Signal Hysteresis
# למעלה: GitHub Actions מריץ תהליך חדש בכל פעם). בכל ריצה הבאה של
# אותו יום, משווים את הקריאה העדכנית לבייסליין: אם RSI/מחיר/VWAP%
# מראים לפחות שני סימני התאוששות (או סימן אחד + הציון עצמו יורד) —
# זו בדיוק תבנית ה-bounce שה-backtest חשף, וההתראה נשארת "ראשונית",
# לא "מאושרת". אם האינדיקטורים לא מתאוששים — ההתדרדרות "מאושרת",
# והבייסליין מתעדכן לנקודה הגרועה ביותר שנראתה היום (כדי שהתאוששות
# עתידית תימדד מול השפל, לא מול הקריאה הראשונה).
# =========================================================

RECOVERY_TOLERANCE_PCT = 0.5   # % סבילות במחיר/VWAP% לפני שנחשב "התאוששות"
RSI_RECOVERY_TOLERANCE = 3.0   # נקודות RSI סבילות לפני שנחשב "התאוששות"


def get_exit_baselines(service):
    """
    קורא בייסליינים של החמרה תוך-יומית מ-Drive.
    מחזיר dict: {ticker: {"date","score","rsi","price","vwap_pct"}}.
    מחזיר {} בכל כשל (שקט, לא עוצר את הריצה).
    """
    if service is None or not EXIT_BASELINE_FILE_ID or "PASTE_YOUR" in EXIT_BASELINE_FILE_ID:
        return {}
    try:
        res = service.files().export_media(fileId=EXIT_BASELINE_FILE_ID, mimeType='text/plain').execute()
        content = res.decode('utf-8-sig', errors='replace').strip()
        if not content:
            return {}
        baselines = {}
        for line in content.split('\n'):
            clean = line.lstrip('﻿​‌‍ ').strip()
            parts = clean.split(',')
            if len(parts) != 6:
                continue
            try:
                ticker, date_str, score, rsi, price, vwap_pct = parts
                baselines[ticker.strip()] = {
                    "date": re.sub(r'[^\d\-]', '', date_str),
                    "score": float(score), "rsi": float(rsi),
                    "price": float(price), "vwap_pct": float(vwap_pct),
                }
            except ValueError:
                continue
        return baselines
    except Exception as e:
        log_event("WARN", "get_exit_baselines", "read failed - returning empty", error=str(e)[:120])
        return {}


def upsert_exit_baselines(service, baselines):
    """
    כותב את כל מפת הבייסליינים בחזרה ל-Drive (overwrite מלא — קובץ קטן,
    לכל היותר טיקר אחד לכל יום, אין צורך בהיסטוריה מצטברת כמו ב-Signal
    History). נקרא רק כשהמפה השתנתה בפועל באותה ריצה.
    """
    if service is None or not EXIT_BASELINE_FILE_ID or "PASTE_YOUR" in EXIT_BASELINE_FILE_ID:
        return
    try:
        lines = [
            f"{t},{b['date']},{b['score']},{b['rsi']},{b['price']},{b['vwap_pct']}"
            for t, b in baselines.items()
        ]
        content = '\n'.join(lines)
        media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain')
        service.files().update(fileId=EXIT_BASELINE_FILE_ID, media_body=media).execute()
    except Exception as e:
        log_event("WARN", "upsert_exit_baselines", "write failed - baseline not updated", error=str(e)[:120])


def track_exit_deterioration(ticker, signal, score, metrics, baselines, today_str):
    """
    לא סופר ריצות — משווה את הקריאה העדכנית לבייסליין (הרגע שבו המניה
    סומנה EXIT RISK/CAUTION לראשונה היום) על בסיס האינדיקטורים עצמם.

    מחזיר: (is_confirmed, baseline_changed, baselines)
      is_confirmed=True  -> ההתדרדרות נמשכת בפועל (לא התאוששה)
      is_confirmed=False -> קריאה ראשונה היום (אין עדיין בסיס להשוואה),
                             או שהאינדיקטורים מראים סימני התאוששות —
                             בדיוק תבנית ה-oversold bounce שה-backtest חשף
    """
    existing = baselines.get(ticker)
    is_today = existing is not None and existing.get("date") == today_str

    if signal not in ("🔴 EXIT RISK", "🟠 CAUTION"):
        if ticker in baselines:  # התאוששות מלאה ל-OK -> מנקים בייסליין
            del baselines[ticker]
            return False, True, baselines
        return False, False, baselines

    if not is_today:
        baselines[ticker] = {
            "date": today_str, "score": float(score), "rsi": float(metrics["rsi"]),
            "price": float(metrics["curr_p"]), "vwap_pct": float(metrics["vwap_pct"]),
        }
        return False, True, baselines

    rsi_recovering = metrics["rsi"] > existing["rsi"] + RSI_RECOVERY_TOLERANCE
    price_recovering = metrics["curr_p"] > existing["price"] * (1 + RECOVERY_TOLERANCE_PCT / 100)
    vwap_recovering = metrics["vwap_pct"] > existing["vwap_pct"] + RECOVERY_TOLERANCE_PCT
    score_dropping = score < existing["score"]
    recovering_signs = sum([rsi_recovering, price_recovering, vwap_recovering])

    if recovering_signs >= 2 or (recovering_signs >= 1 and score_dropping):
        return False, False, baselines  # תבנית bounce -> לא מאושר

    if score >= existing["score"]:  # עדכון הבייסליין לנקודה הגרועה ביותר עד כה
        baselines[ticker] = {
            "date": today_str, "score": float(score), "rsi": float(metrics["rsi"]),
            "price": float(metrics["curr_p"]), "vwap_pct": float(metrics["vwap_pct"]),
        }
        return True, True, baselines
    return True, False, baselines


def get_early_weakness_alerts(watchlist, service=None):
    if not watchlist:
        return ""
    try:
        spy_session = get_latest_rth_session("SPY", period="5d")
        spy_close = extract_col(spy_session, "Close")
        if spy_close is not None:
            spy_close = spy_close.dropna()
        spy_day_chg = (
            calc_pct_change(float(spy_close.iloc[-1]), float(spy_close.iloc[0]))
            if spy_close is not None and len(spy_close) > 1 else 0.0
        )
    except Exception as e:
        log_event("ERROR", "get_early_weakness_alerts", "spy baseline failed", error=str(e)[:160])
        spy_day_chg = 0.0

    # 🔧 (29/09/2026, NEW2) — היה תאריך ישראל (Asia/Jerusalem); הוחלף
    # לתאריך-מסחר ET, כדי שיישאר עקבי עם is_end_of_day_run() (שגם היא
    # עברה ל-ET) ועם המפתח שנשמר ב-Exit Baselines - אחרת ריצה סמוך
    # לחצות ישראל (שכבר "יום מסחר הבא" בישראל, אבל עדיין אותו יום
    # מסחר ב-NYSE) הייתה יוצרת baseline כפול/לא-עקבי לאותו יום מסחר.
    try:
        from datetime import datetime as _dt5
        today_str = _dt5.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    except Exception:
        today_str = _dt5.now().strftime('%Y-%m-%d')
    baselines = get_exit_baselines(service) if service is not None else {}
    baselines_dirty = False

    alerts = []
    for t in watchlist.keys():
        try:
            # 🔧 (26/09/2026) — תוקן: require_min_weekly_change=False בפועל
            # עוקף כאן את הסינון wk_chg>=5 (קודם הפרמטר לא היה קיים בכלל,
            # וההערה כאן טענה בטעות שהסינון לא חל - בפועל הוא כן חל, ומניה
            # יורדת הייתה מדולגת בשקט לפני שהיא נבדקת). וגם: שימוש ב-
            # exit_risk_signal (לא confidence_signal) — זה ציון ייעודי
            # לזיהוי היחלשות של פוזיציה מוחזקת, לא לדירוג מועמד-קנייה חדש.
            # ראו את התיעוד המלא בהגדרת exit_risk_signal.
            #
            # 🔧 (26/09/2026, סבב שני) — suppress_for_earnings=False: מניה
            # מוחזקת שקרובה לדו"ח **ומתדרדרת** לא אמורה להיעלם מהבדיקה
            # (זו הייתה בדיוק אותה תבנית באג כמו wk_chg<5). קרבה לדו"ח היא
            # סיכון *נוסף* על ההיחלשות, לא סיבה להשתיק אותה - ראו earnings_near.
            metrics, drop_reason = compute_intraday_metrics(
                t, spy_day_chg=spy_day_chg, require_min_weekly_change=False, suppress_for_earnings=False,
            )
            if metrics is None:
                log_event("INFO", "get_early_weakness_alerts", "skipped", ticker=t, reason=drop_reason)
                continue
            signal, sig_score, reasons = exit_risk_signal(
                metrics["rs"], metrics["rvol"], metrics["vwap_pct"], metrics["rsi"],
                metrics["wk_chg"], metrics["day_chg"], metrics["above_ma200"], metrics["dist_ma200"],
                metrics["dist_52w_high"],
            )
            if signal in ("🔴 EXIT RISK", "🟠 CAUTION"):
                is_confirmed, changed, baselines = track_exit_deterioration(
                    t, signal, sig_score, metrics, baselines, today_str,
                )
                baselines_dirty = baselines_dirty or changed
                status_tag = (
                    "🔴🔴 מאושר — ההתדרדרות נמשכת (לא הראתה סימני התאוששות מאז שסומנה)"
                    if is_confirmed else
                    "⏳ ראשוני — עדיין ייתכן היפוך/התאוששות, לבדוק שוב בריצה הבאה"
                )
                earnings_note = " | ⚠️ דו\"ח קרוב (≤5 ימי מסחר) — סיכון מוגבר" if metrics.get("earnings_near") else ""
                alerts.append(
                    f"⚠️ {t}: {signal} (ציון {sig_score}) — {', '.join(reasons)}{earnings_note}\n"
                    f"    {status_tag}\n"
                    f"    RS(vsSPY)={metrics['rs']:+.1f} | RVol={metrics['rvol']:.1f}x | Day%={metrics['day_chg']:+.1f}% | Wk%={metrics['wk_chg']:+.1f}%\n"
                    f"    RSI={metrics['rsi']:.0f} | VWAP%={metrics['vwap_pct']:+.1f}% | מעל MA200={'כן' if metrics['above_ma200'] else 'לא'}"
                )
            else:
                # החזרה ל-OK -> מנקים בייסליין אם היה קיים (התאוששות מלאה)
                _, changed, baselines = track_exit_deterioration(
                    t, signal, sig_score, metrics, baselines, today_str,
                )
                baselines_dirty = baselines_dirty or changed
        except Exception as e:
            log_event("ERROR", "get_early_weakness_alerts", "check failed", ticker=t, error=str(e)[:160])

    if baselines_dirty and service is not None:
        upsert_exit_baselines(service, baselines)

    if not alerts:
        return ""
    return (
        "\n⚠️⚠️ EARLY WEAKNESS ALERTS — לפני Stop Loss! ⚠️⚠️\n"
        "------------------------------------------------\n"
        + "\n".join(alerts) + "\n"
        "------------------------------------------------\n"
        "👉 אלו לא Stop Loss שנחצה - אלו סימני היחלשות אמיתית (RS/נפח/מגמה) "
        "שהמחיר עדיין לא בהכרח שיקף. 🔴🔴 מאושר = ראוי לתשומת לב מיידית. "
        "⏳ ראשוני = שווה לעקוב, אך ייתכן שזו קפיצה זמנית (ראו ממצא ה-backtest).\n"
    )


def build_underdog_list(service):
    underdogs = []
    for prefix, bucket in [("Golden_Plan_STOCKS", "STOCKS")]:  # ETF הוסר
        df, status, _fname = download_latest_file(service, prefix)
        if df is None:
            log_event("WARN", "build_underdog_list", "missing output", prefix=prefix, status=status)
            continue
        try:
            tcol, sel, scol = validate_output_schema(df, prefix)
            mask = ~df[sel].astype(str).str.contains(SELECTION_PATTERN, na=False, case=False)
            for _, row in df[mask].iterrows():
                t = str(row[tcol]).strip().upper()
                score = row.get(scol, np.nan) if scol else np.nan
                if not t:
                    continue

                # ✅ Tier_Score — משקל מ-V7
                tier_score = safe_float(row.get("Tier_Score", np.nan), 50.0)

                # ✅ Short_Warning — סינון HIGH_SHORT
                short_warn = str(row.get("Short_Warning", "")).upper()
                if "HIGH_SHORT" in short_warn:
                    log_event("INFO", "build_underdog_list", "filtered HIGH_SHORT", ticker=t, warning=short_warn)
                    continue

                underdogs.append((t, bucket, score, tier_score))
        except Exception as e:
            log_event("ERROR", "build_underdog_list", "underdog build failed", prefix=prefix, error=str(e)[:160])
    return underdogs


# =========================================================
# 8. EXECUTION SCAN
# =========================================================
def status_icon(st):
    return {"Brk": "🚀", "Wch": "👀", "Ext": "⚠️", "Bel": "❌"}.get(st, "•")


def confidence_signal(rs, rvol, vwap_pct, rsi, wk_chg, above_ma200, dist_ma200, dist_52w_high):
    """
    Confidence Score מבוסס-מחקר — מבחין בין המשך מומנטום להיפוך.
    מבוסס על: Jegadeesh-Titman (momentum), Jegadeesh-Lehmann (short-term reversal),
    FasterCapital (MA200), Marquette study (52-week high).
    מחזיר: (signal, score, reasons)
    """
    score = 0
    reasons = []

    # 1. RS vs SPY — momentum strength (Jegadeesh-Titman)
    if rs > 10:
        score += 2; reasons.append("RS+++")
    elif rs >= 3:
        score += 1; reasons.append("RS+")
    elif rs < 0:
        score -= 2; reasons.append("RS-")

    # 2. RVol — liquidity-driven move = אמיתי
    if rvol > 1.5:
        score += 2; reasons.append("Vol++")
    elif rvol < 1.0:
        score -= 2; reasons.append("Vol-")

    # 3. VWAP% — מתוח מדי = reversal צפוי
    if 0 <= vwap_pct <= 3:
        score += 1; reasons.append("VWAP-ok")
    elif vwap_pct > 5:
        score -= 2; reasons.append("VWAP-stretched")

    # 4. MA200 — מגמה שלמה (FasterCapital)
    if above_ma200:
        score += 2; reasons.append("MA200+")
    else:
        score -= 2; reasons.append("MA200-")

    # 5. 52-week high — קרוב לשיא = momentum (Marquette)
    if dist_52w_high >= -5:        # תוך 5% מהשיא
        score += 2; reasons.append("near52wH")
    elif dist_52w_high < -20:      # רחוק מהשיא = גאפ זמני
        score -= 1; reasons.append("far52wH")

    # 6. RSI — מומנטום בריא vs קיצוני
    if 50 <= rsi <= 80:
        score += 1; reasons.append("RSI-ok")
    elif rsi > 85:
        score -= 1; reasons.append("RSI-extreme")

    # 7. wk_chg — short-term reversal warning (Jegadeesh-Lehmann)
    if wk_chg > 15:
        score -= 1; reasons.append("wk-sharp")

    # 8. עלייה חדה ללא נפח תומך = מתיחה, לא momentum אמיתי
    if wk_chg > 15 and rvol < 1.5:
        score -= 2; reasons.append("unbacked-move")

    # קביעת signal לפי ציון
    if score >= 5:
        signal = "🟢 BUY"
    elif score >= 2:
        signal = "🟡 WEAK"
    elif score >= -1:
        signal = "⏳ WAIT"
    else:
        signal = "⚪ AVOID"

    return signal, score, reasons


def exit_risk_signal(rs, rvol, vwap_pct, rsi, wk_chg, day_chg, above_ma200, dist_ma200, dist_52w_high):
    """
    🆕 (26/09/2026) — ציון סיכון-יציאה למניה **מוחזקת**, נפרד מ-confidence_signal.

    למה נפרד: confidence_signal() נבנה למטרה ההפוכה — לדרג עד כמה מועמד-רדאר
    חדש הוא הזדמנות-קנייה חזקה (רשם + ל-RS גבוה, RVol גבוה, קרבה לשיא 52
    שבועות, MA200 חיובי). כשבדקתי אם המנגנון היה תופס מניה כמו AMP (שנפלה
    -10.40% באותו שבוע) גיליתי שהוא לא היה: (1) wk_chg שלילי לא מקבל שום
    ניקוד שלילי שם — רק wk_chg>15 (עלייה חדה) נבדק; (2) RVol גבוה מקבל שם
    בונוס תמיד, גם אם הוא נובע ממכירת פאניקה ביום ירוד, כי אין בדיקה מול
    כיוון היום (day_chg); (3) vwap_pct שלילי (מסחר מתחת ל-VWAP - חולשה
    תוך-יומית) לא מקבל שם שום עונש, רק vwap_pct>5 נבדק. כלומר מניה שקורסת
    יכלה לצבור ניקוד "בריא" ולפספס את ההתראה לגמרי.

    כאן ההפך: כל גורם נבדק במפורש לכיוון הסיכון-לרדת, כולל הבחנה בין נפח
    שמאשר עלייה (חיובי) לנפח שמאשר מכירה/הפצה (שלילי, day_chg<0).
    מחזיר: (signal, score, reasons)
    """
    score = 0
    reasons = []

    # 1. RS מול SPY — חולשה יחסית מתמשכת (Jegadeesh-Titman, כיוון הפוך)
    if rs < -10:
        score += 3; reasons.append("RS--- (חולשה קשה מול השוק)")
    elif rs < -3:
        score += 2; reasons.append("RS-- (נחות מהשוק)")
    elif rs > 3:
        score -= 1; reasons.append("RS+ (עדיין מוביל את השוק)")

    # 2. נפח — ההבחנה הקריטית: נפח גבוה ביום ירוד = הפצה/מכירה אמיתית,
    #    לא אישוש של עלייה. זה בדיוק מה ש-confidence_signal לא בדק.
    if day_chg < 0 and rvol > 1.5:
        score += 3; reasons.append("Vol--- (הפצה בנפח גבוה ביום ירוד)")
    elif day_chg < 0 and rvol > 1.0:
        score += 1; reasons.append("Vol- (נפח מוגבר ביום ירוד)")
    elif day_chg > 0 and rvol > 1.5:
        score -= 1; reasons.append("Vol+ (נפח קונים אמיתי)")

    # 3. VWAP% — מסחר מתחת ל-VWAP תוך-יומי = המוכרים שולטים היום
    if vwap_pct < -3:
        score += 2; reasons.append("VWAP-- (מתחת ל-VWAP באופן משמעותי)")
    elif vwap_pct < -1:
        score += 1; reasons.append("VWAP- (מתחת ל-VWAP)")
    elif vwap_pct > 1:
        score -= 1; reasons.append("VWAP+ (הקונים מגנים על המחיר)")

    # 4. MA200 — שבירת מגמה ארוכת-טווח
    if not above_ma200:
        score += 2; reasons.append("MA200-- (שבר מגמה ארוכת טווח)")
    elif 0 <= dist_ma200 < 3:
        score += 1; reasons.append("MA200~ (קרוב לשבירה)")

    # 5. RSI — היחלשות מומנטום (לא "oversold=קנה", אלא "חולשה נמשכת")
    if rsi < 35:
        score += 2; reasons.append("RSI-- (מומנטום חלש מאוד)")
    elif rsi < 45:
        score += 1; reasons.append("RSI- (מומנטום נחלש)")
    elif rsi > 55:
        score -= 1; reasons.append("RSI+ (מומנטום עדיין בריא)")

    # 6. wk_chg — הגורם שהיה חסר לגמרי ב-confidence_signal לכיוון הזה.
    #    זה בדיוק התרחיש של AMP: ירידה שבועית חדה היא איתות סיכון ישיר,
    #    לא ניטרלי.
    if wk_chg < -8:
        score += 3; reasons.append(f"WK-שבועי {wk_chg:+.1f}% (ירידה חדה)")
    elif wk_chg < -3:
        score += 1; reasons.append(f"WK-שבועי {wk_chg:+.1f}%")

    if score >= 7:
        signal = "🔴 EXIT RISK"
    elif score >= 4:
        signal = "🟠 CAUTION"
    else:
        signal = "🟢 OK"

    return signal, score, reasons


def calc_rank(sw, score_val, wk_chg, rvol, rs, vwap_pct, rsi, status, tier_score=50.0):
    score_part      = safe_float(score_val, 0.0) / 12.0
    week_part       = max(min(wk_chg, 25.0), 0.0) / 6.0
    # 🔧 עודכן (31/07/2026) — היה max(rs, 0.0) בלי תקרה עליונה. rvol_part ו-
    # week_part כן היו עם תקרה (3.0 ו-25.0 בהתאמה), אבל rs_part לא — יום RS
    # קיצוני יחיד (למשל 30-40, שכזכור קורה בדיוק סביב דוחות/חדשות — אותו
    # סוג אירוע שכבר ראינו שפוגע ב-BJRI/CROX/ARQT) יכול היה לייצר rank
    # שמנפח פי כמה מכל שאר הרכיבים גם יחד ולעקוף אפילו את משקל ה-sw*10.
    # התקרה (15.0, כמו טווח ה-RS הסביר של "מומנטום אמיתי" ב-confidence_signal
    # שם הרף העליון rs>10 כבר נחשב המקסימום) מיישרת את זה עם שאר הרכיבים.
    rs_part         = min(max(rs, 0.0), 15.0) * 1.5
    rvol_part       = min(max(rvol, 0.0), 3.0)
    rsi_balance     = max(0.0, 60.0 - abs(rsi - 60.0)) / 20.0
    extension_penalty = max(vwap_pct - 2.0, 0.0) * 1.5
    if status == "Ext":
        extension_penalty += max(vwap_pct - 1.0, 0.0) * 0.75
    # ✅ Tier_Score מ-V7 — משקל 15% מהציון הכולל
    tier_part = safe_float(tier_score, 50.0) / 100.0 * 1.5
    return sw * 10 + score_part + week_part + rs_part + rvol_part + rsi_balance + tier_part - extension_penalty


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
        # Ext FIRST — overheated/extended, avoid chasing
        if (wk_chg >= 15 and vwap_pct >= 3.0) or (vwap_pct >= 2.5 and (rsi >= 65 or rvol >= 2.0)):
            return "Ext", 1, "Extended — avoid chasing"
        # Best breakout structure
        if rs > 0 and rvol >= 1.1 and rsi >= 52 and -0.5 <= vwap_pct <= 1.5:
            return "Brk", 3, "Best breakout structure"
        # Constructive watch
        if wk_chg >= 5 and (rs > 0 or rsi >= 50 or vwap_pct > -1.0):
            return "Wch", 2, "Constructive watch setup"
        return "Bel", 0, "Below threshold"

    if rs > 0 and rvol >= 1.2 and rsi >= 55 and -0.5 <= vwap_pct <= 1.5:
        return "Brk", 2, "Breakout candidate"
    if wk_chg >= 5 and (rs > 0 or rsi >= 50 or vwap_pct > -1.0):
        return "Wch", 2, "Constructive watch setup"
    if (wk_chg >= 15) or (vwap_pct >= 1.5 and (rsi >= 60 or rvol >= 1.5)):
        return "Ext", 1, "Extended"
    return "Bel", 0, "Below threshold"


def is_earnings_trap_radar(ticker, safe_mode=False):
    """
    🔧 חדש (31/07/2026) — הרדאר היומי (build_underdog_list/run_execution_scan)
    לא היה מוגן בכלל מפני דוחות מתקרבים — בניגוד ל-Golden_Plan שכבר תוקן.
    זו אותה בדיקה בדיוק (BDay(5), אותם 3 שדות yfinance נכונים), מועתקת הנה
    כדי שגם מועמדי הרדאר (ה-100 שנסרקים כל יום) יקבלו את אותה הגנה.
    safe_mode=False (בניגוד ל-Golden_Plan) כדי שכשל ברשת לא יחסום את כל
    הרדאר היומי בטעות — כאן מדובר ברשימת מעקב, לא בהמלצת קנייה ישירה.
    """
    try:
        from datetime import datetime as _dt_ert  # מקומי, כמו בכל שאר הקובץ
        t = yf.Ticker(ticker)
        info = t.info
        now = _dt_ert.now()
        window_end = now + pd.tseries.offsets.BDay(5)
        for key in ('earningsTimestampStart', 'earningsTimestamp', 'earningsTimestampEnd'):
            ts = info.get(key)
            if ts:
                earn_date = _dt_ert.fromtimestamp(ts)
                if now <= earn_date <= window_end:
                    return True
        return False
    except Exception:
        return safe_mode


def compute_intraday_metrics(ticker, spy_day_chg=0.0, require_min_weekly_change=True, suppress_for_earnings=True):
    drop_reason = None

    # ── Earnings Trap Guard (חדש, 31/07/2026) ──────────────────────
    # אותו חלון 5 ימי-מסחר נגללים שכבר אושר ל-Golden_Plan. מניה שעומדת
    # לדווח בקרוב לא אמורה להיכנס לרדאר היומי כמועמדת "פריצה הבאה" —
    # בדיוק אותו סיכון (BJRI/CROX) שכבר טיפלנו בו בצד השני של המערכת.
    #
    # 🔧 (26/09/2026) — נמצא תוך כדי בדיקה יזומה (המשתמש שאל "יש עוד מה
    # לבדוק?"): השער הזה, בדיוק כמו wk_chg<5 שתוקן קודם, נכון למועמד-רדאר
    # חדש (לא נכון להיכנס למניה שעומדת לדווח - מונע גם התלהבות ממדד
    # שהוא בעצם רק ציפייה לדוחות) אבל **שגוי** למניה מוחזקת: אם AMP הייתה
    # קרובה לדו"ח וגם מתדרדרת, השער הזה היה מדלג עליה **לגמרי** - בדיוק
    # אותה תבנית באג. למניה מוחזקת, קרבה לדו"ח היא סיכון *נוסף*, לא סיבה
    # להחשיך. תוקן: suppress_for_earnings=True נשאר ברירת המחדל (משפיע רק
    # על run_execution_scan, ההיגיון שם עדיין נכון), get_early_weakness_alerts
    # מעביר False ומוסיף את הקרבה לדו"ח כהערה בטקסט ההתראה, לא כחסימה.
    if suppress_for_earnings and is_earnings_trap_radar(ticker):
        return None, "earnings_trap_5d"
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

    # Day% נכון: מסגירת אתמול (כמו ברוקרים), לא מפתיחת היום
    prev_close_df = get_cached_yf_download(ticker, period="5d", interval="1d")
    prev_close_s = extract_col(prev_close_df, "Close")
    if prev_close_s is not None:
        prev_close_s = prev_close_s.dropna()
    prev_p = safe_float(prev_close_s.iloc[-2], day_open) if prev_close_s is not None and len(prev_close_s) >= 2 else day_open
    day_chg = calc_pct_change(curr_p, prev_p)   # מסגירת אתמול ✅

    wk_open = safe_float(get_week_start_open(ticker), 0.0)
    if wk_open <= 0:
        return None, "missing_week_open"
    wk_chg = calc_pct_change(curr_p, wk_open)
    # 🔧 באג קריטי שתוקן (26/09/2026) — עד עכשיו השורה הבאה הייתה תמיד
    # פעילה (בלי שום פרמטר), גם כשנקראה מ-get_early_weakness_alerts. כלומר
    # מניה מוחזקת שנופלת (למשל AMP, wk_chg=-10.4%) הייתה מקבלת כאן
    # "week_change_below_threshold" ומדלגת *לפני* שהיא בכלל מגיעה ל-
    # confidence_signal — בדיוק המקרה שההתראה הזו נועדה לתפוס. ההערה
    # שהייתה ב-get_early_weakness_alerts טענה בטעות שהסינון עוקף כאן -
    # בפועל הוא לא היה. עכשיו: run_execution_scan (מחפש "פריצה הבאה")
    # ממשיך לדרוש wk_chg>=5 כמו קודם; get_early_weakness_alerts מעביר
    # require_min_weekly_change=False כדי שמניה מוחזקת תמיד תיבדק, גם
    # (ובעיקר) כשהיא יורדת.
    if require_min_weekly_change and wk_chg < 5:
        return None, "week_change_below_threshold"

    # משיכת שנה שלמה — לחישוב RVol + MA200 + 52week high (משיכה אחת)
    hist_daily = get_cached_yf_download(ticker, period="1y", interval="1d")
    hist_vol_s = extract_col(hist_daily, "Volume")
    hist_close_s = extract_col(hist_daily, "Close")
    if hist_close_s is not None:
        hist_close_s = hist_close_s.dropna()

    if hist_vol_s is not None and len(hist_vol_s) >= 5:
        avg_daily_vol = safe_float(hist_vol_s.iloc[-20:-1].mean(), 0.0)  # 20 ימים אחרונים
    else:
        avg_daily_vol = safe_float(volume_s.mean(), 0.0) * len(volume_s)
    today_total_vol = safe_float(volume_s.sum(), 0.0)
    rvol = (today_total_vol / avg_daily_vol) if avg_daily_vol > 0 else 0.0
    rs = day_chg - spy_day_chg

    # ✅ MA200 — מגמה ארוכת טווח (מהמחקר)
    if hist_close_s is not None and len(hist_close_s) >= 200:
        ma200 = safe_float(hist_close_s.iloc[-200:].mean(), 0.0)
        above_ma200 = curr_p > ma200 if ma200 > 0 else False
        dist_ma200 = calc_pct_change(curr_p, ma200) if ma200 > 0 else 0.0
    else:
        # אם אין 200 ימים — נשתמש בכל מה שיש
        ma200 = safe_float(hist_close_s.mean(), 0.0) if hist_close_s is not None and len(hist_close_s) > 0 else 0.0
        above_ma200 = curr_p > ma200 if ma200 > 0 else True
        dist_ma200 = calc_pct_change(curr_p, ma200) if ma200 > 0 else 0.0

    # ✅ 52-week high — מרחק משיא שנתי (מהמחקר)
    if hist_close_s is not None and len(hist_close_s) > 0:
        high_52w = safe_float(hist_close_s.max(), curr_p)
        dist_52w_high = calc_pct_change(curr_p, high_52w) if high_52w > 0 else 0.0
    else:
        dist_52w_high = 0.0

    vol_sum = safe_float(volume_s.sum(), 0.0)
    high_s   = extract_col(session_df, "High")
    low_s    = extract_col(session_df, "Low")
    if high_s is not None and low_s is not None and not high_s.empty and not low_s.empty:
        typical_s = (high_s + low_s + close_s) / 3.0
    else:
        typical_s = close_s
    vwap = safe_float((volume_s * typical_s).sum(), curr_p) / vol_sum if vol_sum > 0 else curr_p
    vwap_pct = calc_pct_change(curr_p, vwap) if vwap > 0 else 0.0
    rsi = calc_intraday_rsi(close_s)

    # אם suppress_for_earnings=True והגענו לכאן, is_earnings_trap_radar כבר
    # נבדק למעלה והחזיר False (אחרת היינו עושים return מוקדם) - אין צורך
    # לבדוק שוב ולחסוך קריאת רשת. אם False, עדיין לא נבדק - בודקים כאן,
    # רק כדי לצרף כמידע (לא כדי לחסום).
    earnings_near = False if suppress_for_earnings else is_earnings_trap_radar(ticker)

    return {
        "curr_p": curr_p,
        "day_chg": day_chg,
        "wk_chg": wk_chg,
        "rvol": rvol,
        "rs": rs,
        "vwap_pct": vwap_pct,
        "rsi": rsi,
        "earnings_near": earnings_near,
        "above_ma200": above_ma200,
        "dist_ma200": dist_ma200,
        "dist_52w_high": dist_52w_high,
    }, drop_reason


def run_execution_scan(service, regime="NEUTRAL", market_note=""):
    underdogs = build_underdog_list(service)
    rows = []
    drop_counts = {}

    # ✅ Hysteresis (29/07/2026) — נטען פעם אחת לכל הריצה, לא לכל מניה בנפרד
    is_eod = is_end_of_day_run()
    # 🔧 (29/09/2026, NEW2) — היה תאריך ישראל; הוחלף לתאריך-מסחר ET, ראו
    # הערה מפורטת ב-run_.. הזהה למעלה (get_early_weakness_alerts) - אותו
    # רציונל: עקביות עם is_end_of_day_run() ועם המפתח ב-Signal History.
    from datetime import datetime as _dt4
    try:
        today_str = _dt4.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    except Exception:
        today_str = _dt4.now().strftime('%Y-%m-%d')
    signal_history = get_signal_history(service) if service is not None else {}
    eod_updates = {}

    spy_session = get_latest_rth_session("SPY", period="5d")
    spy_close = extract_col(spy_session, "Close")
    if spy_close is not None:
        spy_close = spy_close.dropna()  # ניקוי NaN לפני חישוב
    spy_day_chg = calc_pct_change(float(spy_close.iloc[-1]), float(spy_close.iloc[0])) if spy_close is not None and len(spy_close) > 1 else 0.0

    for t, bucket, score, tier_score in underdogs:
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
            rank = calc_rank(sw, score_val, metrics["wk_chg"], metrics["rvol"], metrics["rs"], metrics["vwap_pct"], metrics["rsi"], status, tier_score=tier_score)

            # ✅ Confidence Signal מבוסס-מחקר
            signal, sig_score, sig_reasons = confidence_signal(
                metrics["rs"], metrics["rvol"], metrics["vwap_pct"], metrics["rsi"],
                metrics["wk_chg"], metrics["above_ma200"], metrics["dist_ma200"],
                metrics["dist_52w_high"]
            )

            # ── Hysteresis (תוקן 29/07/2026) — persistence אמיתי ל-Drive ──────
            # לפי יום מסחר (23:15 ישראל), לא לפי ספירת ריצות תוך-יומיות —
            # ראה הערת התיקון המלאה ליד get_signal_history/confirm_buy_signal.
            signal, _is_confirmed = confirm_buy_signal(signal, sig_score, t, signal_history, today_str)
            if is_eod:
                eod_updates[t] = (today_str, sig_score)
            # ────────────────────────────────────────────────────────────────

            rows.append((
                t, bucket, metrics["curr_p"], metrics["day_chg"], metrics["wk_chg"],
                score_val, metrics["rvol"], metrics["rs"], metrics["vwap_pct"], metrics["rsi"],
                status, rank, signal, sig_score
            ))
        except Exception as e:
            drop_counts["execution_exception"] = drop_counts.get("execution_exception", 0) + 1
            log_event("ERROR", "run_execution_scan", "candidate failed", ticker=t, error=str(e)[:160])

    # ✅ שמירה ל-Drive רק בריצת סוף-יום, ורק אם היה מה לעדכן
    if is_eod and eod_updates and service is not None:
        upsert_signal_history(service, eod_updates)

    rows.sort(key=lambda x: x[-1], reverse=True)
    rows = rows[:TOP_N]

    title = f"🎯 Execution Scan — UnderRadar | TOP {TOP_N} | Regime: {regime}"
    if market_note:
        title += f" | {market_note}"

    lines = [
        title,
        "************************** HOT STOCKS ************************",
        "Ticker | Type | Price | Day% | Wk% | Score | RVol | RS | VWAP% | RSI | St | Rank | SIGNAL",
        "**************************************************************",
    ]

    if not rows:
        lines.append("None")
    else:
        for t, bucket, p, d, w, sc, rvol, rs, vwap, rsi, st, rk, signal, sig_score in rows:
            icon = status_icon(st)
            lines.append(
                f"{icon} {t:<5} | {bucket:<6} | {p:>6.2f} | {d:>+5.1f}% | {w:>+5.1f}% | "
                f"{sc:>5.1f} | {rvol:>4.1f}x | {rs:>+4.1f} | {vwap:>+5.1f}% | {rsi:>3.0f} | {st:<3} | {rk:>5.2f} | {signal}"
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
    # 🔧 תוקן 29/07/2026 — ממצא קריטי: main() לא הייתה מוגנת בשום try/except.
    # אם כל שלב באמצע נכשל (למשל run_execution_scan) — כל הריצה קרסה, ולא
    # נשלחה שום הודעה לטלגרם ולא נכתב שום דבר ל-Daily Log, גם לא לחלקים
    # שכן הצליחו לפני הכשל. עכשיו כל שלב מוגן בנפרד עם ברירת מחדל, כדי
    # שכשל חלקי לא ימנע דיווח של השאר.
    validate_environment()
    DATA_CACHE.clear()  # איפוס cache בכל ריצה — מבטיח נתונים טריים מ-yfinance

    try:
        service = get_drive_service()
    except Exception as e:
        service = None
        log_event("ERROR", "main", "get_drive_service failed - continuing without Drive", error=str(e)[:160])

    try:
        watchlist, drive_logs, golden_file_dt = build_dynamic_watchlist(service)
    except Exception as e:
        watchlist, drive_logs, golden_file_dt = {}, "⚠️ build_dynamic_watchlist failed", None
        log_event("ERROR", "main", "build_dynamic_watchlist failed", error=str(e)[:160])

    try:
        dashboard = get_market_dashboard()
    except Exception as e:
        dashboard = "⚠️ Dashboard failed to load"
        log_event("ERROR", "main", "get_market_dashboard failed", error=str(e)[:160])
    dashboard += f"\n🔍 Diagnostics:\n{drive_logs}\n"

    try:
        portfolio = get_portfolio_performance(watchlist, golden_file_dt, service=service)
    except Exception as e:
        portfolio = "⚠️ Portfolio report failed this run — see logs"
        log_event("ERROR", "main", "get_portfolio_performance failed", error=str(e)[:160])

    try:
        regime, market_note = get_market_regime()
    except Exception as e:
        regime, market_note = "UNKNOWN", ""
        log_event("ERROR", "main", "get_market_regime failed", error=str(e)[:160])

    try:
        early_alerts = get_early_weakness_alerts(watchlist, service=service)
    except Exception as e:
        early_alerts = ""
        log_event("ERROR", "main", "get_early_weakness_alerts failed", error=str(e)[:160])

    try:
        execution_scan = run_execution_scan(service, regime=regime, market_note=market_note)
    except Exception as e:
        execution_scan = "⚠️ Execution Scan failed this run — see logs"
        log_event("ERROR", "main", "run_execution_scan failed", error=str(e)[:160])

    if SHOW_DEBUG:
        try:
            dashboard += "\n🧪 Debug Summary:\n" + get_debug_summary(15) + "\n"
        except Exception as e:
            log_event("ERROR", "main", "get_debug_summary failed", error=str(e)[:160])

    # ✅ (26/09/2026) — מעקב אחרי הצלחת/כשל כתיבת ה-Daily Log ל-Drive.
    # למה זה חדש: log_to_drive עד עכשיו רק הדפיסה שגיאה לקונסול של
    # GitHub Actions (שאף אחד לא פותח בזמן אמת) - הטלגרם (הערוץ שבאמת
    # עוקבים אחריו) לא ידע כלום על כשל בכתיבה ל-Daily Log. עכשיו: כל
    # קריאה נרשמת ל-drive_log_results, ובסוף הריצה - אם משהו נכשל -
    # נשלחת התראה מפורשת בטלגרם עם הסיבה האמיתית (מתוך DEBUG_EVENTS).
    drive_log_results = []

    # ✅ שני הבלוקים נשלחים/נרשמים ללא תלות אחד בשני — כשל באחד לא חוסם את האחר
    try:
        send_msg(f"{dashboard}\n{portfolio}")
    except Exception as e:
        log_event("ERROR", "main", "send_msg (dashboard+portfolio) failed", error=str(e)[:160])
    try:
        ok = log_to_drive(service, f"{dashboard}\n{portfolio}")
        drive_log_results.append(("dashboard+portfolio", ok))
    except Exception as e:
        drive_log_results.append(("dashboard+portfolio", False))
        log_event("ERROR", "main", "log_to_drive (dashboard+portfolio) failed", error=str(e)[:160])

    # ✅ בלוק עצמאי (26/09/2026) — כשל כאן לא חוסם dashboard/portfolio/execution_scan
    if early_alerts:
        try:
            send_msg(early_alerts)
        except Exception as e:
            log_event("ERROR", "main", "send_msg (early_alerts) failed", error=str(e)[:160])
        try:
            ok = log_to_drive(service, early_alerts)
            drive_log_results.append(("early_alerts", ok))
        except Exception as e:
            drive_log_results.append(("early_alerts", False))
            log_event("ERROR", "main", "log_to_drive (early_alerts) failed", error=str(e)[:160])

    try:
        send_msg(execution_scan)
    except Exception as e:
        log_event("ERROR", "main", "send_msg (execution_scan) failed", error=str(e)[:160])
    try:
        ok = log_to_drive(service, execution_scan)
        drive_log_results.append(("execution_scan", ok))
    except Exception as e:
        drive_log_results.append(("execution_scan", False))
        log_event("ERROR", "main", "log_to_drive (execution_scan) failed", error=str(e)[:160])

    # ✅ (26/09/2026) — התראה גלויה בטלגרם אם ה-Daily Log נכשל, עם הסיבה
    # האמיתית (מהחריגה שנתפסה), כדי שכשל לא יתגלה רק כשמנסים לשלוף
    # היסטוריה שבועית ומגלים שהיא ריקה.
    failed_writes = [name for name, ok in drive_log_results if not ok]
    if failed_writes:
        recent_log_errors = [
            f"- {ev['where']}: {ev.get('error', ev['message'])}"
            for ev in DEBUG_EVENTS
            if ev.get("level") == "ERROR" and ev.get("where") in ("log_to_drive", "get_drive_service")
        ][-5:]
        alert_text = (
            "🚨 DAILY LOG (Drive) — כשל כתיבה!\n"
            f"נכשלו {len(failed_writes)}/{len(drive_log_results)}: {', '.join(failed_writes)}\n"
        )
        if recent_log_errors:
            alert_text += "\nסיבה (מה-log):\n" + "\n".join(recent_log_errors)
        try:
            send_msg(alert_text)
        except Exception as e:
            log_event("ERROR", "main", "send_msg (drive log failure alert) failed", error=str(e)[:160])


if __name__ == "__main__":
    main()
