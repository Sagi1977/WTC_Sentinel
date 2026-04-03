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

# --- הגדרות ליבה ---
TOKEN    = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID  = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

# ═══════════════════════════════════════════════════════
#  פונקציות עזר
# ═══════════════════════════════════════════════════════

def send_telegram_msg(text):
    if not text: return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "Markdown"}
    res = requests.post(url, json=payload)
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
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh  = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python')
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

def extract_col(df, col_name):
    """
    תמיכה ב-yfinance >= 0.2.x (MultiIndex) וגם גרסאות ישנות.
    """
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        level0 = df.columns.get_level_values(0)
        if col_name in level0:
            return df[col_name].iloc[:, 0]
        return None
    return df[col_name] if col_name in df.columns else None

def filter_rth(df):
    """
    מסנן נרות לשעות מסחר רגילות בלבד: 09:30–16:00 ET.
    חיוני למניעת פרי-מרקט / אפטר-מרקט שיזייפו Open, High ו-Status.
    """
    if df is None or df.empty:
        return df
    try:
        idx = df.index
        et_idx = idx.tz_convert('America/New_York') if (hasattr(idx, 'tz') and idx.tz) else idx
        mask = (
            ((et_idx.hour == 9) & (et_idx.minute >= 30)) |
            ((et_idx.hour > 9)  & (et_idx.hour < 16))
        )
        return df[mask]
    except Exception:
        return df

def get_current_price(ticker):
    """
    מחיר עדכני מנרות 5 דק' RTH — מדויק עד 5 דקות.
    לא משתמשים בנרות שעה כי הם יכולים להיות ישנים עד 59 דקות.
    """
    try:
        d5    = yf.download(ticker, period="1d", interval="5m", progress=False)
        d5_rth = filter_rth(d5)
        close = extract_col(d5_rth, 'Close')
        if close is not None and not close.empty:
            return float(close.iloc[-1]), d5_rth
        return None, d5_rth
    except Exception:
        return None, None

def get_wk_open(ticker, mon_date):
    """
    Open של נר 09:30 ET ביום שני — בסיס ל-Wk%.
    מריץ filter_rth כדי לוודא שלא לוקחים נר פרי-מרקט של יום שני.
    """
    try:
        wk_raw = yf.download(ticker, start=mon_date.strftime('%Y-%m-%d'),
                             interval="1h", progress=False)
        wk_rth  = filter_rth(wk_raw)          # ← חיוני: מסנן פרי-מרקט של יום שני
        open_wk = extract_col(wk_rth, 'Open')
        if open_wk is not None and not open_wk.empty:
            return float(open_wk.iloc[0])      # Open 09:30 ET יום שני
        return None
    except Exception:
        return None

# ═══════════════════════════════════════════════════════
#  1. בניית רשימה דינמית מ-Drive
# ═══════════════════════════════════════════════════════

def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean_cols = {c: re.sub(r'[^a-zA-Z0-9]', '', str(c)).lower() for c in df.columns}
            df = df.rename(columns=clean_cols)
            sel_col    = next((c for c in df.columns if 'final' in c or 'selection' in c), None)
            ticker_col = next((c for c in df.columns if 'ticker' in c), 'ticker')
            if sel_col:
                mask     = df[sel_col].astype(str).str.contains('Anchor|Turbo|Top 5', na=False, case=False)
                filtered = df[mask]
                for _, row in filtered.iterrows():
                    t = str(row[ticker_col]).strip().upper()
                    watchlist[t] = str(row[sel_col])
                logs.append(f"✅ {prefix}: Found {len(filtered)}")
            else:
                logs.append(f"⚠️ {prefix}: Col Selection missing")
        else:
            logs.append(f"❌ {prefix}: {status}")
    return watchlist, "\n".join(logs)

# ═══════════════════════════════════════════════════════
#  2. טבלת פורטפוליו
#
#  Day%   = curr_p  vs. Open 09:30 ET היום         (נרות 5m RTH)
#  Wk%    = curr_p  vs. Open 09:30 ET יום שני      (נרות 1h RTH)
#  Status = curr_p >= High של 6 נרות ראשוני RTH   (09:30–10:00 ET)
# ═══════════════════════════════════════════════════════

def get_portfolio_performance(watchlist):
    if not watchlist:
        return "⚠️ Watchlist empty - Check CSV labels.\n"

    now_isr    = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_mon = now_isr.weekday()   # שני=0, שלישי=1 ... שישי=4, שבת=5, ראשון=6
    mon_date   = (now_isr - datetime.timedelta(days=days_to_mon)).date()

    report  = "📈 *WTC Portfolio Watch*\n"
    report += "`Type       | Ticker | Price  | Day%  | Wk%   | Status`\n"
    report += "`----------------------------------------------------`\n"

    for t, label in watchlist.items():
        try:
            # ── 1. מחיר עדכני + DataFrame 5m RTH של היום ──────────────
            curr_p, d5_rth = get_current_price(t)
            if curr_p is None:
                report += f"`{'N/D':<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️ NoData`\n"
                continue

            # ── 2. Day%: Open 09:30 ET היום ───────────────────────────
            open_d5 = extract_col(d5_rth, 'Open')
            if open_d5 is not None and not open_d5.empty:
                day_open = float(open_d5.iloc[0])
            else:
                day_open = curr_p   # fallback — Day% יהיה 0%
            day_chg = ((curr_p / day_open) - 1) * 100

            # ── 3. Wk%: Open 09:30 ET יום שני ─────────────────────────
            wk_open = get_wk_open(t, mon_date)
            if wk_open is None:
                wk_open = day_open  # fallback — Wk% = Day%
            wk_chg = ((curr_p / wk_open) - 1) * 100

            # ── 4. Status: curr_p vs. High 09:30–10:00 ET ─────────────
            high_d5 = extract_col(d5_rth, 'High')
            if high_d5 is not None and len(high_d5) >= 6:
                open_high = float(high_d5.iloc[:6].max())
            elif high_d5 is not None and not high_d5.empty:
                open_high = float(high_d5.max())
            else:
                open_high = curr_p

            status     = "✅ Break" if curr_p >= open_high else "❌ Below"
            type_label = (label[:7] + ".") if len(label) > 8 else label[:8]

            report += (
                f"`{type_label:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )

        except Exception:
            report += f"`{'Err':<8} | {t:<5} | N/A    | N/A   | N/A   | ❌ Err`\n"
            continue

    report += "`----------------------------------------------------`\n"
    return report

# ═══════════════════════════════════════════════════════
#  3. ניתוח AI מוסדי
# ═══════════════════════════════════════════════════════

def get_ai_report(custom_prompt=None):
    try:
        news = ""
        for t in ["^GSPC", "^VIX"]:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        p = custom_prompt if custom_prompt else (
            f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
            f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n"
            f"### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        )
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except:
        return "⚠️ AI Summary Unavailable"

# ═══════════════════════════════════════════════════════
#  4. סריקת פריצות — רק מניות מהווatchlist, RTH בלבד
# ═══════════════════════════════════════════════════════

def run_execution_scan(watchlist):
    """
    סורק פריצות רק על מניות מה-watchlist (Anchor/Turbo/Top 5).
    פריצה = curr_p > High של 6 נרות ראשוני RTH (09:30–10:00 ET).
    """
    res = {"STOCKS": [], "ETF": []}

    # מיפוי label → קטגוריה
    for t, label in watchlist.items():
        category = "ETF" if "Top 5 E" in label or "ETF" in label.upper() else "STOCKS"
        try:
            curr_p, d5_rth = get_current_price(t)
            if curr_p is None: continue
            high_s = extract_col(d5_rth, 'High')
            if high_s is None or len(high_s) < 7: continue
            if curr_p > float(high_s.iloc[:6].max()):
                res[category].append(t)
        except:
            continue

    vix     = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
    report  = "🎯 *Execution Scan Result:*\n"
    report += f"🥇 STOCKS: {', '.join(res['STOCKS']) or 'None'}\n"
    report += f"🏅 ETF: {', '.join(res['ETF']) or 'None'}\n\n"
    if not res["STOCKS"] and not res["ETF"]:
        report += "💡 *סיכום טכני:* השוק בדשדוש; אין פריצות מעל גבוה הבוקר."
        if vix > 22: report += " להמתין ל-VIX."
    else:
        total = len(res['STOCKS']) + len(res['ETF'])
        report += f"🚀 *סיכום טכני:* זוהו פריצות מומנטום ב-{total} נכסים."
    return report

# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════

def main():
    service   = get_drive_service()
    now       = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour      = now.hour
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'

    watchlist, drive_logs = build_dynamic_watchlist(service)

    # SPY — שינוי יומי vs. Open 09:30 ET (5m RTH)
    try:
        spy_5m  = yf.download("SPY", period="1d", interval="5m", progress=False)
        spy_rth = filter_rth(spy_5m)
        spy_close = extract_col(spy_rth, 'Close')
        spy_open  = extract_col(spy_rth, 'Open')
        s_p = float(spy_close.iloc[-1])
        s_c = ((s_p / float(spy_open.iloc[0])) - 1) * 100
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
