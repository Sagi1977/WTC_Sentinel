import os
import datetime
import time
import pandas as pd
import yfinance as yf
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai
import google.auth
import io
import re

TOKEN      = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID    = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

# ═══════════════════════════════════════════════════
#  פונקציות עזר
# ═══════════════════════════════════════════════════

def send_telegram_msg(text):
    if not text: return
    url     = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "Markdown"}
    res     = requests.post(url, json=payload)
    if res.status_code != 200:
        requests.post(url, json={"chat_id": CHAT_ID, "text": text[:4000]})
    time.sleep(1.2)

def get_drive_service():
    creds, _ = google.auth.default()
    return build('drive', 'v3', credentials=creds)

def download_latest_file(service, prefix):
    try:
        query = f"name contains '{prefix}'"
        res   = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, f"❓ {prefix} Missing"
        fh  = io.BytesIO()
        MediaIoBaseDownload(fh, service.files().get_media(fileId=files[0]['id'])).next_chunk()
        fh.seek(0)
        return pd.read_csv(fh, encoding='utf-8-sig', engine='python'), "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

def extract_col(df, col_name):
    """תמיכה ב-MultiIndex (yfinance >= 0.2.x) וגם index רגיל."""
    if df is None or df.empty: return None
    if isinstance(df.columns, pd.MultiIndex):
        lvl = df.columns.get_level_values(0)
        return df[col_name].iloc[:, 0] if col_name in lvl else None
    return df[col_name] if col_name in df.columns else None

def filter_rth(df):
    """מסנן נרות RTH בלבד: 09:30–16:00 ET. מונע פרי/אפטר-מרקט."""
    if df is None or df.empty: return df
    try:
        idx    = df.index
        et_idx = idx.tz_convert('America/New_York') if (hasattr(idx, 'tz') and idx.tz) else idx
        mask   = (((et_idx.hour == 9) & (et_idx.minute >= 30)) |
                  ((et_idx.hour > 9)  & (et_idx.hour < 16)))
        return df[mask]
    except Exception:
        return df

def get_5m_rth(ticker, period='1d'):
    """מוריד נרות 5 דקות ומסנן RTH."""
    try:
        raw = yf.download(ticker, period=period, interval='5m', progress=False)
        return filter_rth(raw)
    except Exception:
        return None

def find_open_at_or_after(df, target_hour, target_minute):
    """
    מחזיר את ערך ה-Open של הנר הראשון שמגיע ב-target_hour:target_minute ET או אחריו.
    """
    if df is None or df.empty: return None
    open_s = extract_col(df, 'Open')
    if open_s is None or open_s.empty: return None
    idx    = df.index
    et_idx = idx.tz_convert('America/New_York') if (hasattr(idx, 'tz') and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            return float(open_s.iloc[i])
    return None

def get_monday_10am_open(ticker):
    """
    Open 10:00 ET של יום שני — עוגן שבועי.
    מוריד 5 ימים של נרות 5 דקות RTH, מאתר את יום שני ולוקח את נר 10:00.
    """
    try:
        df = get_5m_rth(ticker, period='5d')
        if df is None or df.empty: return None
        et_idx        = df.index.tz_convert('America/New_York') if (hasattr(df.index, 'tz') and df.index.tz) else df.index
        monday_mask   = [ts.weekday() == 0 for ts in et_idx]
        monday_df     = df[monday_mask]
        return find_open_at_or_after(monday_df, 10, 0)
    except Exception:
        return None

# ═══════════════════════════════════════════════════
#  1. בניית רשימה דינמית מ-Drive
# ═══════════════════════════════════════════════════

def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean = {c: re.sub(r'[^a-zA-Z0-9]', '', str(c)).lower() for c in df.columns}
            df    = df.rename(columns=clean)
            sel   = next((c for c in df.columns if 'final' in c or 'selection' in c), None)
            tcol  = next((c for c in df.columns if 'ticker' in c), 'ticker')
            if sel:
                mask = df[sel].astype(str).str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                for _, row in df[mask].iterrows():
                    watchlist[str(row[tcol]).strip().upper()] = str(row[sel])
                logs.append(f"✅ {prefix}: Found {mask.sum()}")
            else:
                logs.append(f"⚠️ {prefix}: Col Selection missing")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# ═══════════════════════════════════════════════════
#  2. טבלת פורטפוליו
#
#  Day%   = curr_p vs. Open 09:30 ET היום
#  Wk%    = curr_p vs. Open 10:00 ET יום שני
#  Status = ✅ Break אם Wk% >= 0  |  ❌ Below אם Wk% < 0
# ═══════════════════════════════════════════════════

def get_portfolio_performance(watchlist):
    if not watchlist:
        return "⚠️ Watchlist empty - Check CSV labels.\n"

    report  = "📈 *WTC Portfolio Watch*\n"
    report += "`Type       | Ticker | Price  | Day%  | Wk%   | Status`\n"
    report += "`----------------------------------------------------`\n"

    for t, label in watchlist.items():
        try:
            # ── נתוני היום (5m RTH) ────────────────────────────────────
            d5_today = get_5m_rth(t, period='1d')
            close_s  = extract_col(d5_today, 'Close')
            if close_s is None or close_s.empty:
                report += f"`{'N/D':<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️ NoData`\n"
                continue

            curr_p = float(close_s.iloc[-1])

            # ── Day%: Open 09:30 ET היום ───────────────────────────────
            day_open = find_open_at_or_after(d5_today, 9, 30)
            if day_open is None: day_open = curr_p
            day_chg = ((curr_p / day_open) - 1) * 100

            # ── Wk%: Open 10:00 ET יום שני ────────────────────────────
            wk_open = get_monday_10am_open(t)
            if wk_open is None: wk_open = day_open
            wk_chg = ((curr_p / wk_open) - 1) * 100

            # ── Status: פשוט סימן של Wk% ──────────────────────────────
            status     = "✅ Break" if wk_chg >= 0 else "❌ Below"
            type_label = (label[:7] + ".") if len(label) > 8 else label[:8]

            report += (
                f"`{type_label:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )

        except Exception:
            report += f"`{'Err':<8} | {t:<5} | N/A    | N/A   | N/A   | ❌ Err`\n"

    report += "`----------------------------------------------------`\n"
    return report

# ═══════════════════════════════════════════════════
#  3. ניתוח AI מוסדי
# ═══════════════════════════════════════════════════

def get_ai_report(custom_prompt=None):
    try:
        news = ""
        for t in ["^GSPC", "^VIX"]:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        p = custom_prompt or (
            f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
            f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n"
            f"### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        )
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except:
        return "⚠️ AI Summary Unavailable"

# ═══════════════════════════════════════════════════
#  4. סריקת פריצות — Status = Break מה-watchlist
# ═══════════════════════════════════════════════════

def run_execution_scan(watchlist):
    """
    מדווח על מניות שה-Wk% שלהן חיובי (Status = Break).
    משתמש באותו עוגן שבועי בדיוק כמו הטבלה.
    """
    res = {"STOCKS": [], "ETF": []}
    for t, label in watchlist.items():
        category = "ETF" if "Top 5 E" in label or "ETF" in label.upper() else "STOCKS"
        try:
            d5_today = get_5m_rth(t, period='1d')
            close_s  = extract_col(d5_today, 'Close')
            if close_s is None or close_s.empty: continue
            curr_p  = float(close_s.iloc[-1])
            wk_open = get_monday_10am_open(t)
            if wk_open is None: continue
            if curr_p >= wk_open:
                res[category].append(t)
        except:
            continue

    vix    = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
    report = "🎯 *Execution Scan Result:*\n"
    report += f"🥇 STOCKS: {', '.join(res['STOCKS']) or 'None'}\n"
    report += f"🏅 ETF: {', '.join(res['ETF']) or 'None'}\n\n"
    if not res["STOCKS"] and not res["ETF"]:
        report += "💡 *סיכום טכני:* אין נכסים מעל עוגן השבוע."
        if vix > 22: report += " VIX גבוה — זהירות."
    else:
        total   = len(res['STOCKS']) + len(res['ETF'])
        report += f"🚀 *סיכום טכני:* {total} נכסים מעל עוגן שבוע המסחר."
    return report

# ═══════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════

def main():
    service   = get_drive_service()
    now       = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour      = now.hour
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'

    watchlist, drive_logs = build_dynamic_watchlist(service)

    # SPY Day% = Open 09:30 ET היום
    try:
        spy_d5  = get_5m_rth("SPY", period='1d')
        spy_cls = extract_col(spy_d5, 'Close')
        s_p     = float(spy_cls.iloc[-1])
        spy_opn = find_open_at_or_after(spy_d5, 9, 30)
        s_c     = ((s_p / spy_opn) - 1) * 100 if spy_opn else 0.0
    except:
        s_p, s_c = 0.0, 0.0

    vix_val      = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
    status_label = 'BEARISH' if vix_val > 25 else 'CAUTION' if vix_val > 18 else 'BULLISH'

    header = (
        f"📊 *WTC Sentinel Dashboard*\n"
        f"`--------------------------`\n"
        f"🚦 Status: `{status_label}` | VIX: `{vix_val:.2f}`\n"
        f"📈 SPY: `{s_p:.2f} ({s_c:+.2f}%)`\n"
        f"`--------------------------`\n\n"
        f"🔍 *Diagnostics:*\n`{drive_logs}`\n"
    )
    perf = get_portfolio_performance(watchlist)

    if is_manual:
        send_telegram_msg(f"{header}\n{perf}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(watchlist))
        return

    if hour == 16:
        send_telegram_msg(f"{header}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{header}\n{perf}\n{run_execution_scan(watchlist)}")
    elif hour == 23:
        send_telegram_msg(f"{header}🌙 *Closing Summary*\n\n{get_ai_report('סכם את יום המסחר.')}")

if __name__ == "__main__":
    main()
