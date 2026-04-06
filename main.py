import os
import datetime
import time
import pandas as pd
import yfinance as yf
import requests
import pytz
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google import genai
import google.auth
import io
import re

TOKEN    = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID  = os.environ.get('TELEGRAM_CHAT_ID')
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
        query = f"name contains '{prefix}' and mimeType = 'text/csv'"
        res = service.files().list(q=query, orderBy="createdTime desc").execute()
        files = res.get('files', [])
        if not files: return None, "Missing"
        file_id = files[0]['id']
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        MediaIoBaseDownload(fh, req).next_chunk()
        fh.seek(0)
        df = pd.read_csv(fh)
        df.columns = [c.strip().capitalize() for c in df.columns]
        return df, "Success"
    except: return None, "Error"

# ─── עזר: MultiIndex + RTH ────────────────────────────────────────
def extract_col(df, col_name):
    if df is None or df.empty: return None
    if isinstance(df.columns, pd.MultiIndex):
        lvl = df.columns.get_level_values(0)
        return df[col_name].iloc[:, 0] if col_name in lvl else None
    return df[col_name] if col_name in df.columns else None

def filter_rth(df):
    if df is None or df.empty: return df
    try:
        idx = df.index
        et_idx = idx.tz_convert('America/New_York') if (hasattr(idx, 'tz') and idx.tz) else idx
        mask = (((et_idx.hour == 9) & (et_idx.minute >= 30)) |
                ((et_idx.hour > 9) & (et_idx.hour < 16)))
        return df[mask]
    except: return df

def get_5m_rth(ticker, period='1d'):
    try:
        raw = yf.download(ticker, period=period, interval='5m', progress=False)
        return filter_rth(raw)
    except: return None

def find_open_at_or_after(df, target_hour, target_minute):
    if df is None or df.empty: return None
    open_s = extract_col(df, 'Open')
    if open_s is None or open_s.empty: return None
    idx = df.index
    et_idx = idx.tz_convert('America/New_York') if (hasattr(idx, 'tz') and idx.tz) else idx
    for i, ts in enumerate(et_idx):
        if ts.hour > target_hour or (ts.hour == target_hour and ts.minute >= target_minute):
            return float(open_s.iloc[i])
    return None

def get_monday_10am_open(ticker):
    try:
        df = get_5m_rth(ticker, period='5d')
        if df is None or df.empty: return None
        et_idx = df.index.tz_convert('America/New_York') if (hasattr(df.index, 'tz') and df.index.tz) else df.index
        monday_mask = [ts.weekday() == 0 for ts in et_idx]
        monday_df = df[monday_mask]
        return find_open_at_or_after(monday_df, 10, 0)
    except: return None

# ─── 1. Watchlist דינמי ───────────────────────────────────────────
def build_dynamic_watchlist(service):
    watchlist, logs = {}, []
    for prefix in ["Golden_Plan_STOCKS", "Golden_Plan_ETF"]:
        df, _ = download_latest_file(service, prefix)
        if df is not None:
            target_col = next((c for c in df.columns if 'Final' in c), None)
            if target_col:
                filtered = df[df[target_col].str.contains('Anchor|Turbo|Top 5', na=False, case=False)]
                for _, row in filtered.iterrows():
                    ticker = str(row['Ticker']).strip()
                    watchlist[ticker] = {
                        "type": str(row[target_col]).split('(')[0].strip(),
                        "score": row.get('Score', 0)
                    }
                logs.append(f"✅ {prefix}: Found {len(filtered)}")
            else:
                logs.append(f"⚠️ {prefix}: Col missing")
        else:
            logs.append(f"❌ {prefix}: Error")
    return watchlist, "\n".join(logs)

# ─── 2. Dashboard — SPY Day% נכון (vs Open 09:30 ET) ────────────
def get_market_dashboard():
    try:
        spy_5m  = get_5m_rth("SPY", period='1d')
        spy_cls = extract_col(spy_5m, 'Close')
        s_p     = float(spy_cls.iloc[-1])
        spy_opn = find_open_at_or_after(spy_5m, 9, 30)
        s_c     = ((s_p / spy_opn) - 1) * 100 if spy_opn else 0.0

        vix    = yf.Ticker("^VIX").history(period="1d")
        v_p    = float(vix['Close'].iloc[-1])
        status = "BULLISH" if v_p < 18 else "CAUTION" if v_p < 25 else "BEARISH"
        emoji  = "🟢" if status == "BULLISH" else "⚠️" if status == "CAUTION" else "🔴"
        return (
            f"📊 *WTC Sentinel Dashboard*\n"
            f"`--------------------------`\n"
            f"🚦 *Status:* `{status}` {emoji}\n"
            f"📉 *VIX:* `{v_p:.2f}` | 📈 *SPY:* `{s_p:.2f} ({s_c:+.2f}%)`\n"
            f"`--------------------------`\n"
        )
    except: return "⚠️ Dashboard Offline\n\n"

# ─── 3. Portfolio — Day% ו-Wk% נכונים ───────────────────────────
def get_portfolio_snapshot(watchlist):
    if not watchlist: return "⚠️ לא נמצאו נכסי מפתח בקבצים.\n"
    report  = "📈 *My Portfolio Watch (Dynamic)*\n"
    report += "`Type       | Ticker | Price  | Day%  | Wk%   | Status`\n"
    report += "`--------------------------------------------------`\n"
    for t, info in watchlist.items():
        try:
            d5_today = get_5m_rth(t, period='1d')
            close_s  = extract_col(d5_today, 'Close')
            if close_s is None or close_s.empty:
                report += f"`{info['type'][:8]:<8} | {t:<5} | N/A    | N/A   | N/A   | ⚠️`\n"
                continue
            curr_p   = float(close_s.iloc[-1])

            day_open = find_open_at_or_after(d5_today, 9, 30)
            if day_open is None: day_open = curr_p
            day_chg  = ((curr_p / day_open) - 1) * 100

            wk_open  = get_monday_10am_open(t)
            if wk_open is None: wk_open = day_open
            wk_chg   = ((curr_p / wk_open) - 1) * 100

            status   = "✅ Break" if wk_chg >= 0 else "❌ Below"
            ttype    = info['type'][:8]
            report  += (
                f"`{ttype:<8} | {t:<5} | {curr_p:>6.2f} | "
                f"{day_chg:>+5.1f}% | {wk_chg:>+5.1f}% | {status}`\n"
            )
        except:
            report += f"`{'Err':<8} | {t:<5} | N/A    | N/A   | N/A   | ❌`\n"
    report += "`--------------------------------------------------`\n"
    return report + "\n"

# ─── 4. AI Report ─────────────────────────────────────────────────
def get_ai_report(custom_prompt=None):
    news = ""
    for t in ["^GSPC", "^VIX"]:
        try:
            for n in yf.Ticker(t).news[:2]:
                title = n.get('title') or n.get('content', {}).get('title')
                if title: news += f"- {title}\n"
        except: continue
    prompt = custom_prompt if custom_prompt else (
        f"ענה בעברית כמחלקת מחקר גולדמן סאקס. נתח: {news}\n"
        f"מבנה: ## דוח אסטרטגי\n### 🏛️ 1. הכסף הגדול\n"
        f"### 💣 2. מוקשים ומאקרו\n### 🌡️ 3. סנטימנט"
    )
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        target = next((m.name for m in client.models.list() if 'flash' in m.name), 'gemini-1.5-flash')
        return client.models.generate_content(model=target, contents=prompt).text
    except: return "⚠️ שגיאת AI בניתוח."

# ─── 5. Execution Scan ────────────────────────────────────────────
def run_execution_scan(service):
    results = {"STOCKS": [], "ETF": []}
    log     = ""
    mapping = {"Golden_Plan_STOCKS": "STOCKS", "Golden_Plan_ETF": "ETF"}
    for prefix, label in mapping.items():
        df, status = download_latest_file(service, prefix)
        log += f"{prefix}: {status}\n"
        if df is not None and 'Ticker' in df.columns:
            for _, row in df.iterrows():
                t, s = str(row['Ticker']).strip(), row.get('Score', 0)
                try:
                    d5    = get_5m_rth(t, period='1d')
                    cls_s = extract_col(d5, 'Close')
                    hi_s  = extract_col(d5, 'High')
                    if cls_s is None or len(cls_s) < 7: continue
                    if float(cls_s.iloc[-1]) > float(hi_s.iloc[:6].max()):
                        results[label].append(f"{t}({s})")
                except: continue
    res_msg  = f"🎯 *WTC Execution Scan Result:*\n{log}\n"
    res_msg += f"🥇 *STOCKS Gold:* {', '.join(results['STOCKS']) or 'None'}\n"
    res_msg += f"🏅 *ETF Gold:*    {', '.join(results['ETF'])    or 'None'}\n\n"
    if not results["STOCKS"] and not results["ETF"]:
        v_val    = float(yf.Ticker("^VIX").history(period="1d")['Close'].iloc[-1])
        res_msg += "💡 *סטטוס:* השוק בלחץ; נסחרים מתחת לגבוה היומי." if v_val > 22 else "💡 *סטטוס:* חוסר מומנטום בפריצות."
    return res_msg

# ─── MAIN ──────────────────────────────────────────────────────────
def main():
    service   = get_drive_service()
    isr_tz    = pytz.timezone('Asia/Jerusalem')
    now       = datetime.datetime.now(datetime.timezone.utc).astimezone(isr_tz)
    hour      = now.hour
    minute    = now.minute
    is_manual = os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch'

    watchlist, drive_logs = build_dynamic_watchlist(service)
    db        = get_market_dashboard()
    portfolio = get_portfolio_snapshot(watchlist)
    db       += f"\n🔍 *Diagnostics:*\n`{drive_logs}`\n"

    if is_manual:
        send_telegram_msg(f"{db}\n{portfolio}")
        send_telegram_msg(get_ai_report())
        send_telegram_msg(run_execution_scan(service))
        return

    if hour == 16:
        send_telegram_msg(f"{db}\n{get_ai_report()}")
    elif 17 <= hour <= 20:
        if minute <= 10:
            send_telegram_msg(f"{db}\n{portfolio}")
            send_telegram_msg(run_execution_scan(service))
    elif hour == 23:
        closing = "סכם בעברית את יום המסחר ב
