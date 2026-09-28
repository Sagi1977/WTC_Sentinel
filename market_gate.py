# -*- coding: utf-8 -*-
"""
market_gate.py — שומר-סף לפני main.py ב-GitHub Actions.

למה זה קיים:
  GitHub Actions מבין cron רק ב-UTC. שוק ניו יורק זז שעה ב-UTC פעמיים
  בשנה (שעון קיץ/חורף), וישראל עוברת בתאריכים אחרים. לכן:
  1. sentinel_run.yml מגדיר cron לשני הגרסאות (EDT וגם EST).
  2. הסקריפט הזה ממיר את הזמן *המתוכנן* של הטריגר לשעון ניו יורק,
     ומריץ רק אם זה אחד מהסלוטים שהגדרנו (SLOTS_ET) — כל טריגר של
     "הגרסה השנייה" מדלג אוטומטית. אין צורך לגעת בקובץ פעמיים בשנה.
  3. בודק שזה יום מסחר של NYSE (חגים + ימים מקוצרים).
  4. מדלג על טריגר שהתעכב יותר מדי (GitHub לפעמים מעכב בעומס).

פלט (ל-$GITHUB_OUTPUT):
  run=true|false
  kind=INTRADAY|EOD
  slot_et=HH:MM      slot_il=HH:MM      reason=...

בדיקה מקומית:
  WTC_EVENT=schedule WTC_SCHEDULE='30 13 * * 1-5' WTC_NOW_UTC=2026-09-28T13:34 python market_gate.py
"""
import os
import sys
from datetime import datetime, timedelta, date, time as dtime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:  # Python < 3.9
    from backports.zoneinfo import ZoneInfo  # type: ignore

ET = ZoneInfo("America/New_York")
IL = ZoneInfo("Asia/Jerusalem")

# ── הסלוטים — מוגדרים בשעון ניו יורק, לא ב-UTC ולא בשעון ישראל ──────────
# בשעון ישראל (רוב השנה, הפרש 7 שעות): 16:30, 16:45, 17:00, 17:30 ... 22:30, 23:15
INTRADAY_SLOTS_ET = ["09:30", "09:45", "10:00"] + [
    f"{h:02d}:{m:02d}" for h in range(10, 16) for m in (0, 30)
    if (h, m) >= (10, 30) and (h, m) <= (15, 30)
]
EOD_SLOT_ET = "16:15"

# עיכוב מקסימלי מותר בין הזמן המתוכנן לזמן הריצה בפועל
MAX_LAG_MIN = {"INTRADAY": 40, "EOD": 240}

# ── גיבוי אם pandas_market_calendars לא זמין (לוודא מול nyse.com מדי שנה) ──
FALLBACK_HOLIDAYS = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26", "2027-05-31",
    "2027-06-18", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
}
FALLBACK_EARLY_CLOSE = {"2026-11-27": "13:00", "2026-12-24": "13:00",
                        "2027-11-26": "13:00"}


def _out(**kv):
    """כותב ל-GITHUB_OUTPUT (או מדפיס בהרצה מקומית) + סיכום קריא."""
    path = os.environ.get("GITHUB_OUTPUT")
    lines = [f"{k}={v}" for k, v in kv.items()]
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    for line in lines:
        print(line)
    summ = os.environ.get("GITHUB_STEP_SUMMARY")
    if summ:
        with open(summ, "a", encoding="utf-8") as f:
            icon = "▶️" if kv.get("run") == "true" else "⏭️"
            f.write(f"### {icon} Market gate: run={kv.get('run')} ({kv.get('kind','')})\n\n"
                    f"- Slot: {kv.get('slot_et','-')} ET / {kv.get('slot_il','-')} Israel\n"
                    f"- Reason: {kv.get('reason','')}\n")


def _now_utc():
    override = os.environ.get("WTC_NOW_UTC")
    if override:
        return datetime.fromisoformat(override).replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def _session_close(d: date):
    """מחזיר שעת סגירה (ET) ליום מסחר, או None אם השוק סגור."""
    try:
        import pandas_market_calendars as mcal
        sched = mcal.get_calendar("NYSE").schedule(start_date=d, end_date=d)
        if sched.empty:
            return None
        return sched.iloc[0]["market_close"].tz_convert(ET).time()
    except Exception as e:
        print(f"⚠️ pandas_market_calendars unavailable ({e}) — using fallback list")
        if d.weekday() >= 5 or d.isoformat() in FALLBACK_HOLIDAYS:
            return None
        hhmm = FALLBACK_EARLY_CLOSE.get(d.isoformat(), "16:00")
        return dtime.fromisoformat(hhmm)


def _intended_utc_from_cron(cron: str, now: datetime) -> datetime:
    """'30 13 * * 1-5' → הזמן המתוכנן ב-UTC (היום, או אתמול אם עוד לא הגיע)."""
    minute, hour = cron.split()[:2]
    if not (minute.isdigit() and hour.isdigit()):
        raise ValueError(f"cron must be a single fixed time, got: {cron!r}")
    t = now.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)
    if t > now + timedelta(minutes=1):     # עיכוב שחצה חצות UTC
        t -= timedelta(days=1)
    return t


def main():
    event = os.environ.get("WTC_EVENT", "")
    now = _now_utc()

    # ── הרצה ידנית: תמיד רצה. סוג הריצה לפי בחירה או לפי שעון ניו יורק ──
    if event != "schedule":
        forced = os.environ.get("WTC_FORCE_KIND", "auto").upper()
        et_now = now.astimezone(ET)
        kind = forced if forced in ("INTRADAY", "EOD") else (
            "EOD" if et_now.time() >= dtime(16, 0) else "INTRADAY")
        return _out(run="true", kind=kind,
                    slot_et=et_now.strftime("%H:%M"),
                    slot_il=now.astimezone(IL).strftime("%H:%M"),
                    reason=f"manual ({event or 'local'}), kind={kind}")

    # ── הרצה מתוזמנת ─────────────────────────────────────────────────────
    cron = os.environ.get("WTC_SCHEDULE", "").strip()
    try:
        intended = _intended_utc_from_cron(cron, now)
    except Exception as e:
        # לא ברור מה הטריגר — עדיף לרוץ מאשר לפספס בשקט
        return _out(run="true", kind="INTRADAY", slot_et="?", slot_il="?",
                    reason=f"cannot parse schedule ({e}) — running anyway")

    et = intended.astimezone(ET)
    slot = et.strftime("%H:%M")
    slot_il = intended.astimezone(IL).strftime("%H:%M")

    if slot == EOD_SLOT_ET:
        kind = "EOD"
    elif slot in INTRADAY_SLOTS_ET:
        kind = "INTRADAY"
    else:
        return _out(run="false", kind="-", slot_et=slot, slot_il=slot_il,
                    reason=f"cron '{cron}' = {slot} ET — belongs to the other DST variant")

    close = _session_close(et.date())
    if close is None:
        return _out(run="false", kind=kind, slot_et=slot, slot_il=slot_il,
                    reason=f"{et.date()} is not an NYSE trading day")
    if kind == "INTRADAY" and et.time() >= close:
        return _out(run="false", kind=kind, slot_et=slot, slot_il=slot_il,
                    reason=f"early close at {close.strftime('%H:%M')} ET")

    lag = (now - intended).total_seconds() / 60
    if lag > MAX_LAG_MIN[kind]:
        return _out(run="false", kind=kind, slot_et=slot, slot_il=slot_il,
                    reason=f"GitHub delayed this trigger by {lag:.0f} min (> {MAX_LAG_MIN[kind]})")

    return _out(run="true", kind=kind, slot_et=slot, slot_il=slot_il,
                reason=f"trading day, lag {lag:.0f} min")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:   # אסור שהשומר עצמו יפיל את הריצה בשקט
        print(f"⚠️ market_gate crashed: {e} — defaulting to run")
        _out(run="true", kind="INTRADAY", slot_et="?", slot_il="?", reason=f"gate error: {e}")
    sys.exit(0)
