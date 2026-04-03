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

TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GEMINI_KEY = os.environ.get('GEMINI_API_KEY')

ET_TZ = 'America/New_York'
ISR_TZ_OFFSET = 3

def send_telegram_msg(text):
    if not text:
        return
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
        if not files:
            return None, f"❓ {prefix} Missing"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh, encoding='utf-8-sig', engine='python')
        return df, "Loaded"
    except Exception as e:
        return None, f"Err: {str(e)[:30]}"

def extract_col(df, col_name):
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        level0 = df.columns.get_level_values(0)
        if col_name in level0:
            return df[col_name].iloc[:, 0]
        return None
    return df[col_name] if col_name in df.columns else None

def filter_rth(df):
    if df is None or df.empty:
        return df
    try:
        idx = df.index
        et_idx = idx.tz_convert(ET_TZ) if (hasattr(idx, 'tz') and idx.tz is not None) else idx
        mask = (((et_idx.hour == 9) & (et_idx.minute >= 30)) | ((et_idx.hour > 9) & (et_idx.hour < 16)))
        return df[mask]
    except Exception:
        return df

def get_et_index(df):
    if df is None or df.empty:
        return None
    idx = df.index
    return idx.tz_convert(ET_TZ) if (hasattr(idx, 'tz') and idx.tz is not None) else idx

def find_bar_value_at_or_after(df, col_name, target_hour, target_minute):
    if df is None or df.empty:
        return None
    series = extract_col(df, col_name)
    if series is None or series.empty:
        return None
    et_idx = get_et_index(df)
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            return float(series.iloc[i])
    return None

def get_today_rth_5m(ticker):
    try:
        d5 = yf.download(ticker, period="1d", interval="5m", progress=False)
        return filter_rth(d5)
    except Exception:
        return None

def get_multi_day_rth_5m(ticker, days='5d'):
    try:
        d5 = yf.download(ticker, period=days, interval="5m", progress=False)
        return filter_rth(d5)
    except Exception:
        return None

def get_current_price_from_today(d5_rth):
    close_s = extract_col(d5_rth, 'Close')
    if close_s is None or close_s.empty:
        return None
    return float(close_s.iloc[-1])

def get_week_anchor_open_10am(ticker):
    d5 = get_multi_day_rth_5m(ticker, '5d')
    if d5 is None or d5.empty:
        return None
    et_idx = get_et_index(d5)
    monday_positions = [i for i, ts in enumerate(et_idx) if ts.weekday() == 0]
    if not monday_positions:
        return None
    monday_df = d5.iloc[monday_positions]
    return find_bar_value_at_or_after(monday_df, 'Open', 10, 0)

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

def get_portfolio_performance(watchlist):
    if not watchlist:
        return "⚠️ Watchlist empty - Check CSV labels.\n"

    report  = "📈 *WTC Portfolio Watch*\n"
    report += "`Type       | Ticker | Price  | Day%  | Wk%   | Status`\n"
    report += "`----------------------------------------------------`\n"

    for t, label in watchlist.items():
        try:
            d5_today = get_today_rth_5m(t)
            if d5_today is None or d5_today.empty:
                report += f"`{'N/D':<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️ NoData`\n"
                continue

            curr_p = get_current_price_from_today(d5_today)
            if curr_p is None:
                report += f"`{'N/D':<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️ NoData`\n"
                continue

            day_open = find_bar_value_at_or_after(d5_today, 'Open', 9, 30)
            if day_open is None:
                day_open = curr_p
            day_chg = ((curr_p / day_open) - 1) * 100

            wk_open = get_week_anchor_open_10am(t)
            if wk_open is None:
                wk_open = day_open
            wk_chg = ((curr_p / wk_open) - 1) * 100

            high_d5 = extract_col(d5_today, 'High')
            if high_d5 is not None and len(high_d5) >= 6:
                open_high = float(high_d5.iloc[:6].max())
            elif high_d5 is not None and not high_d5.empty:
                open_high = float(high_d5.max())
            else:
                open_high = curr_p

            status = "✅ Break" if curr_p >= open_high else "❌ Below"
            type_label = (label[:7] + ".") if len(label) > 8 else label[:8]
            report += f"`{type_label:<8} | {t:<5} | {curr_p:>6.2f} | {day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
        except Exception:
            report += f"`{'Err':<8} | {t:<5} | N/A    | N/A   | N/A   | ❌ Err`\n"
            continue

    report += "`----------------------------------------------------`\n"
    return report

def get_ai_report(custom_prompt=None):
    try:
        news = ""
        for t in ["^GSPC", "^VIX"]:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title:
                    news += f"- {title}\n"
        p = custom_prompt if custom_prompt else f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\nמבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=p).text
    except:
        return "⚠️ AI Summary Unavailable"

def run_execution_scan(watchlist):
    res = {"STOCKS": [], "ETF": []}
    for t, label in watchlist.items():
        category = "ETF" if "Top 5 E" in label or "ETF" in label.upper() else "STOCKS"
        try:
            d5_today = get_today_rth_5m(t)
            curr_p = get_current_price_from_today(d5_today)
            high_s = extract_col(d5_today, 'High')
            if curr_p is None or high_s is None or len(high_s) < 7:
                continue
            if curr_p > float(high_s.iloc[:6].max()):
                res[category].append(t)
        except:
            continue

    vix = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
    report  = "🎯 *Execution Scan Result:*\n"
    report += f"🥇 STOCKS: {', '.join(res['STOCKS']) or 'None'}\n"
    report += f"🏅 ETF: {', '.join(res['ETF']) or 'None'}\n\n"
    if not res["STOCKS"] and not res["ETF"]:
        report += "💡 *סיכום טכני:* השוק בדשדוש; אין פריצות מעל גבוה הבוקר."
        if vix > 22:
            report += " להמתין ל-VIX."
    else:
        report += f"🚀 *סיכום טכני:* זוהו פריצות מומנטום ב-{len(res['STOCKS']) + len(res['ETF'])} נכסים."
    return report

def main():
    service = get_drive_service()
    now = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=ISR_TZ_OFFSET)
    hour = now.hour
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'

    watchlist, drive_logs = build_dynamic_watchlist(service)

    try:
        spy_today = get_today_rth_5m("SPY")
        s_p = get_current_price_from_today(spy_today)
        spy_open = find_bar_value_at_or_after(spy_today, 'Open', 9, 30)
        s_c = ((s_p / spy_open) - 1) * 100 if s_p is not None and spy_open is not None else 0.0
    except:
        s_p, s_c = 0.0, 0.0

    vix_val = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
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
