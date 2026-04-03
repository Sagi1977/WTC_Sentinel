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
TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

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
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, f"❓ {prefix} Missing"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python')
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

# --- FIX: פונקציית עזר לחילוץ עמודה מ-MultiIndex או Index רגיל ---
def extract_col(df, col_name):
    """
    תומך גם ב-yfinance חדש (MultiIndex) וגם ישן (עמודות רגילות).
    """
    if isinstance(df.columns, pd.MultiIndex):
        # yfinance >= 0.2.x מחזיר ('Close', 'TICKER')
        level0 = df.columns.get_level_values(0)
        if col_name in level0:
            return df[col_name].iloc[:, 0]  # קח את הטיקר הראשון
        return None
    else:
        return df[col_name] if col_name in df.columns else None

# --- FIX: פונקציית עזר להשוואת תאריכים עם timezone ---
def filter_today(df, today_date):
    """
    מסנן שורות לפי תאריך תוך התחשבות ב-timezone.
    """
    try:
        if hasattr(df.index, 'tz') and df.index.tz is not None:
            idx_dates = df.index.tz_convert('Asia/Jerusalem').date
        else:
            idx_dates = pd.Series(df.index).dt.date.values
        mask = [d == today_date for d in idx_dates]
        return df[mask]
    except:
        return df[df.index.date == today_date]

# --- 1. בניית רשימה דינמית ---
def build_dynamic_watchlist(service):
    watchlist = {}
    logs = []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, status = download_latest_file(service, prefix)
        if df is not None:
            clean_cols = {c: re.sub(r'[^a-zA-Z0-9]', '', str(c)).lower() for c in df.columns}
            df = df.rename(columns=clean_cols)
            sel_col = next((c for c in df.columns if 'final' in c or 'selection' in c), None)
            ticker_col = next((c for c in df.columns if 'ticker' in c), 'ticker')
            if sel_col:
                mask = df[sel_col].astype(str).str.contains('Anchor|Turbo|Top 5', na=False, case=False)
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

# --- 2. ביצועי פורטפוליו (FIX: MultiIndex + Timezone + Day/Wk%) ---
def get_portfolio_performance(watchlist):
    if not watchlist: return "⚠️ Watchlist empty - Check CSV labels.\n"

    now_isr = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    days_to_mon = now_isr.weekday()
    mon_date = (now_isr - datetime.timedelta(days=days_to_mon)).date()

    report = "📈 *WTC Portfolio Watch*\n"
    report += "`Type       | Ticker | Price | Day%  | Wk%   | Status`\n"
    report += "`--------------------------------------------------`\n"

    for t, label in watchlist.items():
        try:
            # --- FIX 1: הורדת נתוני שבוע ---
            wk_raw = yf.download(t, start=mon_date.strftime('%Y-%m-%d'), interval="1h", progress=False)

            if wk_raw.empty:
                report += f"`{label[:8]:<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️ NoData`\n"
                continue

            # --- FIX 2: תמיכה ב-MultiIndex ---
            close_s = extract_col(wk_raw, 'Close')
            open_s  = extract_col(wk_raw, 'Open')

            if close_s is None or open_s is None:
                report += f"`{label[:8]:<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️ ColErr`\n"
                continue

            curr_p = float(close_s.iloc[-1])
            wk_open = float(open_s.iloc[0])
            wk_chg = ((curr_p / wk_open) - 1) * 100

            # --- FIX 3: Day% עם timezone-aware filtering ---
            today_data = filter_today(wk_raw, now_isr.date())
            if not today_data.empty:
                today_open_s = extract_col(today_data, 'Open')
                day_open = float(today_open_s.iloc[0]) if today_open_s is not None and not today_open_s.empty else wk_open
            else:
                day_open = wk_open  # fallback

            day_chg = ((curr_p / day_open) - 1) * 100

            # --- FIX 4: Status מול גבוה הבוקר ---
            d5_raw = yf.download(t, period="1d", interval="5m", progress=False)
            high_s = extract_col(d5_raw, 'High')
            if high_s is not None and len(high_s) >= 6:
                o_high = float(high_s.iloc[:6].max())
            else:
                o_high = curr_p

            status = "✅ Break" if curr_p >= o_high else "❌ Below"
            type_label = label[:8] if len(label) <= 8 else label[:7] + "."

            report += f"`{type_label:<8} | {t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"

        except Exception as e:
            report += f"`{label[:8]:<8} | {t:<5} | N/A    | N/A   | N/A   | ❌ Err`\n"
            continue

    return report + "`--------------------------------------------------`\n"

# --- 3. ניתוח AI מוסדי ---
def get_ai_report(custom_prompt=None):
    try:
        news = ""
        for t in ["^GSPC", "^VIX"]:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        p = custom_prompt if custom_prompt else (
            f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
            f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        )
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except:
        return "⚠️ AI Summary Unavailable"

# --- 4. סריקת פריצות וסיכום טכני ---
def run_execution_scan(service):
    res = {"STOCKS": [], "ETF": []}
    for pref, label in {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}.items():
        df, _ = download_latest_file(service, pref)
        if df is not None:
            t_col = next((c for c in df.columns if 'ticker' in str(c).lower()), 'ticker')
            for _, row in df.iterrows():
                t = str(row.get(t_col, '')).strip()
                try:
                    d_raw = yf.download(t, period="1d", interval="5m", progress=False)
                    close_s = extract_col(d_raw, 'Close')
                    high_s  = extract_col(d_raw, 'High')
                    if close_s is None or high_s is None or len(close_s) < 7:
                        continue
                    if float(close_s.iloc[-1]) > float(high_s.iloc[:6].max()):
                        res[label].append(t)
                except:
                    continue

    vix = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
    report = f"🎯 *Execution Scan Result:*\n🥇 STOCKS: {', '.join(res['STOCKS']) or 'None'}\n🏅 ETF: {', '.join(res['ETF']) or 'None'}\n\n"
    if not res["STOCKS"] and not res["ETF"]:
        report += "💡 *סיכום טכני:* השוק בדשדוש; אין פריצות מעל גבוה הבוקר. " + ("להמתין ל-VIX." if vix > 22 else "")
    else:
        report += f"🚀 *סיכום טכני:* זוהו פריצות מומנטום ב-{len(res['STOCKS']) + len(res['ETF'])} נכסים."
    return report

# --- MAIN ---
def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)
    hour, is_manual = now.hour, os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'

    watchlist, drive_logs = build_dynamic_watchlist(service)

    spy = yf.Ticker("SPY").history(period="2d")
    vix_val = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
    s_p = float(spy['Close'].iloc[-1])
    s_c = ((s_p / float(spy['Close'].iloc[-2])) - 1) * 100
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
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{header}\n{get_ai_report()}")
    elif 17 <= hour < 23:
        send_telegram_msg(f"{header}\n{perf}\n{run_execution_scan(service)}")
    elif hour == 23:
        send_telegram_msg(f"{header}🌙 *Closing Summary*\n\n{get_ai_report('סכם את יום המסחר.')}")

if __name__ == "__main__":
    main()
