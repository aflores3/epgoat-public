#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EPGOAT Linker Prototype — Prefix-only, API-OFF, filename-driven scheduling

- Strictly filters channels by an allowed prefix list (tolerant of numbers/symbols after the prefix).
- No external APIs (by design). Event timing is parsed from the channel name if present.
- Event detection = "payload beyond the shell":
    Shell = <PREFIX> + optional separators + optional number + optional badge + optional delimiter.
    If anything meaningful remains after removing the shell, it's an Event (unless it's in a fluff ignore-list).
- Special-case: "Peacock 01: Studio" is treated as GENERIC per your instruction; otherwise "Studio/Pregame/Postgame"
  payloads are treated as Event (Q3).

Scheduling for a target DATE (default: today in America/Chicago):
  * GENERIC channel (no meaningful payload): fill the day with 2-hour "No Programming Today." blocks.
  * EVENT channel:
      - If a time is parsed AND it's on the target date:
            00:00 → event_start      : "Airing Next: <Title> @ <... CT>"
            event_start → +180 mins  : "❗ <Title> ❗"
            remainder of day         : 2-hour "No Programming Today."
      - If a time is parsed BUT it's NOT on the target date (Q5=A):
            Whole day: "Airing Next: <Title> @ <... CT>"
      - If NO time is parsed (Q2=A):
            Whole day: "Airing Next: <Title> (Time TBA)"
            NOTE: In the future, we'll restore API lookups to resolve exact times when TBA.  <-- per your request

Outputs:
  - XMLTV (UTC timestamps)
  - CSV audit for processed channels only
"""

from __future__ import annotations
import argparse
import csv
import datetime as dt
import hashlib
import json
import re
import sys
import xml.sax.saxutils as saxutils

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ----------------------------
# Allowed prefixes (exactly from your list)
# ----------------------------
ALLOWED_PREFIXES = [
    "Now HK Sports 4K 1 UHD","NCAAF","NFL Game Pass","LIVE EVENT","[PPV EVENT","NBA","USA Real NBA",
    "NHL","NHL |","USA Real NHL","MLB","USA Real MLB","MiLB","MLS","MLS NEXT PRO","USA | MLS",
    "MLS Espanol","USA Soccer","WNBA","Paramount+","ESPN+","SEC+ / ACC extra","Flo Sports","Flo Racing",
    "Flo Football","Prime US","Peacock","US|Peacock PPV","BIG10+","NCAAB","NCAAW B","NCAA Softball",
    "NJCAA Men's Basketball","Dirtvision : EVENT","Fanatiz","Serie A","La Liga","Bundesliga","Ligue1",
    "UEFA Champions League","UEFA Europa League","UEFA Europa Conf. League","UEFA/FIFA",
    "National League TV","FRIENDLY","TrillerTV Event","Hub Sports","Hub Premier","STAN SPORT EVENT",
    "Tennis","Tennis TV","GAAGO : GAME","LOI GAME","Clubber","FIBA","Setanta Sports",
]

# Compile tolerant "shell" patterns for each prefix.
def build_prefix_regex(prefix: str) -> re.Pattern:
    esc = re.escape(prefix)
    # Allow optional separators, optional number (ASCII/full-width), optional ⓧ, optional delimiter, trailing spaces.
    tail = r"(?:\s*[|/])?\s*(?:[0-9\uFF10-\uFF19]+)?\s*(?:ⓧ)?\s*(?:[:\-\|\uFF1A])?\s*"
    return re.compile(r"^" + esc + tail, re.IGNORECASE | re.UNICODE)

PREFIX_PATTERNS = [(p, build_prefix_regex(p)) for p in ALLOWED_PREFIXES]

def match_prefix_and_shell(name: str) -> tuple[bool, str | None, re.Match | None]:
    n = (name or "").strip()
    for prefix, rx in PREFIX_PATTERNS:
        m = rx.match(n)
        if m:
            return True, prefix, m
    return False, None, None

# ----------------------------
# Fluff/Ignore payload tokens (treated as GENERIC if they're all you see)
# You said "trust your judgement" — tuned to avoid false events.
# ----------------------------
FLUFF_TOKENS = {
    "LIVE","TEST","FHD","UHD","HEVC","EN","ES","ALT","MULTI","BACKUP","FEED","EVENT","STREAM",
    "1080P","2160P","4K","HDR","SD","HD","H264","H265","AAC","AC3","EAC3",
}

# Words that *usually* indicate real content (treated as event if present),
# but we will honor your explicit exception for Peacock:Studio below.
CONTENT_HINTS = {"STUDIO","PREGAME","POSTGAME","SHOW","MATCH","GAME","VS","RACE","REPLAY","LIVEBLOG"}

# Special-case overrides (prefix -> set of exact payloads considered GENERIC)
SPECIAL_GENERIC_EXCEPTIONS = {
    "Peacock": {"STUDIO"},
}

# ----------------------------
# Optional time parsing (NOT required to be an event)
# We try several lightweight forms if present; otherwise we stay time-less.
# ----------------------------
EVENT_TIME_RXES = [
    # "Oct 09 08:55 AM ET" or "Oct 9 8:55pm CT"  (month + day + time + AM/PM + TZ)
    re.compile(r"([A-Za-z]{3})\s+(\d{1,2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # NEW: "07:30 PM ET"  (time + AM/PM + TZ; no date)
    re.compile(r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "10/09 20:00 CET"  (MM/DD + 24h time + TZ)
    re.compile(r"(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "20:00 ET"  (24h time + TZ; no date)
    re.compile(r"(\d{1,2}):(\d{2})\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "8pm PT"  (hour + AM/PM + TZ; no minutes, no date)
    re.compile(r"(\d{1,2})\s*(AM|PM)\s*([A-Za-z]{1,4})", re.IGNORECASE),
]


MONTHS = {m: i for i, m in enumerate(
    ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"], start=1
)}

TZ_TO_IANA = {
    "ET":"America/New_York","EST":"America/New_York","EDT":"America/New_York",
    "CT":"America/Chicago","CST":"America/Chicago","CDT":"America/Chicago",
    "MT":"America/Denver","MST":"America/Denver","MDT":"America/Denver",
    "PT":"America/Los_Angeles","PST":"America/Los_Angeles","PDT":"America/Los_Angeles",
    "UTC":"UTC","GMT":"UTC",
}

def try_parse_time(payload: str, year: int, central: ZoneInfo, date_context: dt.date) -> dt.datetime | None:
    """
    Try to find a time in the payload; normalize to Central if found.
    For patterns without an explicit date, we anchor to the provided date_context (the --date).
    """
    text = payload.strip()
    for rx in EVENT_TIME_RXES:
        m = rx.search(text)
        if not m:
            continue

        try:
            if rx is EVENT_TIME_RXES[0]:
                # "Oct 09 08:55 AM ET"
                mon_abbr, day, hh, mm, ampm, tz_abbr = m.groups()
                mon = MONTHS.get(mon_abbr[:3].title())
                if not mon:
                    continue
                hour = int(hh) % 12
                if ampm.upper() == "PM": hour += 12
                minute = int(mm)
                tz = TZ_TO_IANA.get(tz_abbr.upper(), "America/New_York")
                src = ZoneInfo(tz)
                local = dt.datetime(year, mon, int(day), hour, minute, 0, tzinfo=src)
                return local.astimezone(central)

            elif rx is EVENT_TIME_RXES[1]:
                # "07:30 PM ET" (no date → use date_context)
                hh, mm, ampm, tz_abbr = m.groups()
                hour = int(hh) % 12
                if ampm.upper() == "PM": hour += 12
                minute = int(mm)
                tz = TZ_TO_IANA.get(tz_abbr.upper(), "America/New_York")
                src = ZoneInfo(tz)
                local = dt.datetime(date_context.year, date_context.month, date_context.day, hour, minute, 0, tzinfo=src)
                return local.astimezone(central)

            elif rx is EVENT_TIME_RXES[2]:
                # "10/09 20:00 CET"
                mon, day, hh, mm, tz_abbr = m.groups()
                hour = int(hh); minute = int(mm)
                tz = TZ_TO_IANA.get(tz_abbr.upper(), "America/New_York")
                src = ZoneInfo(tz)
                local = dt.datetime(year, int(mon), int(day), hour, minute, 0, tzinfo=src)
                return local.astimezone(central)

            elif rx is EVENT_TIME_RXES[3]:
                # "20:00 ET" (no date → use date_context)
                hh, mm, tz_abbr = m.groups()
                hour = int(hh); minute = int(mm)
                tz = TZ_TO_IANA.get(tz_abbr.upper(), "America/New_York")
                src = ZoneInfo(tz)
                local = dt.datetime(date_context.year, date_context.month, date_context.day, hour, minute, 0, tzinfo=src)
                return local.astimezone(central)

            else:
                # "8pm PT" (no minutes, no date → use date_context)
                hh, ampm, tz_abbr = m.groups()
                hour = int(hh) % 12
                if ampm.upper() == "PM": hour += 12
                tz = TZ_TO_IANA.get(tz_abbr.upper(), "America/New_York")
                src = ZoneInfo(tz)
                local = dt.datetime(date_context.year, date_context.month, date_context.day, hour, 0, 0, tzinfo=src)
                return local.astimezone(central)

        except Exception:
            continue

    return None


# ----------------------------
# XML helpers
# ----------------------------
def xml_esc(s: str) -> str:
    return saxutils.escape(s or "", {'"': "&quot;"})

def chan_id(entry) -> str:
    # Collision-safe: display/tvg name + stable URL token
    base = (entry.tvg_id or entry.tvg_name or entry.display_name or "channel").strip()
    base = re.sub(r"\s+", "_", base)
    tok = hashlib.sha1(entry.url.encode("utf-8")).hexdigest()[:8]
    return f"{base}__{tok}"

def fmt_xmltv_dt(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(dt.timezone.utc).strftime("%Y%m%d%H%M%S +0000")

# ----------------------------
# M3U parsing
# ----------------------------
class M3UEntry:
    def __init__(self, attrs: dict, display_name: str, url: str):
        self.attrs = attrs
        self.display_name = display_name
        self.url = url
    @property
    def tvg_id(self): return self.attrs.get("tvg-id")
    @property
    def tvg_name(self): return self.attrs.get("tvg-name")
    @property
    def group_title(self): return self.attrs.get("group-title")
    @property
    def tvg_logo(self): return self.attrs.get("tvg-logo")

def parse_extinf_attrs(info_line: str):
    if not info_line.startswith("#EXTINF"):
        return {}, info_line.strip()
    try:
        header, disp = info_line.split(",", 1)
    except ValueError:
        header, disp = info_line, ""
    attrs = {k.lower(): v for k, v in re.findall(r'([\w\-]+)="([^"]*)"', header)}
    return attrs, disp.strip()

def parse_m3u(path: str):
    lines = []
    if path.startswith("http://") or path.startswith("https://"):
        import urllib.request
        with urllib.request.urlopen(path) as resp:
            lines = resp.read().decode("utf-8", "replace").splitlines()
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()

    entries = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF"):
            attrs, disp = parse_extinf_attrs(line)
            url, j = "", i + 1
            while j < len(lines):
                nxt = lines[j].strip(); j += 1
                if not nxt or nxt.startswith("#"): continue
                url = nxt; break
            if url:
                entries.append(M3UEntry(attrs, disp, url))
            i = j
        else:
            i += 1
    return entries

# ----------------------------
# Classification helpers
# ----------------------------
def classify_channel(name: str, matched_prefix: str) -> tuple[str, str]:
    """
    Returns (classification, payload).
    classification: "generic" or "event"
    payload: the part after the shell (may be "")
    """
    ok, pref, m = match_prefix_and_shell(name)
    if not ok or not m:
        return "generic", ""

    payload = name[m.end():].strip()

    if not payload:
        return "generic", ""

    # Token normalize
    payload_upper = re.sub(r"\s+", " ", payload).strip().upper()

    # Special-case generic exceptions like "Peacock : Studio"
    if matched_prefix and matched_prefix in SPECIAL_GENERIC_EXCEPTIONS:
        if payload_upper in SPECIAL_GENERIC_EXCEPTIONS[matched_prefix]:
            return "generic", payload

    # If payload is pure fluff tokens (or token + punctuation), treat as generic
    tokens = [t for t in re.split(r"[^A-Z0-9]+", payload_upper) if t]
    if tokens and all(t in FLUFF_TOKENS for t in tokens):
        return "generic", payload

    # Otherwise, it's an event (even if time is missing)
    return "event", payload

# ----------------------------
# Programme building
# ----------------------------
def add_block(programs, cid, title, start_local, end_local, desc=""):
    programs.setdefault(cid, []).append({
        "title": title,
        "start": start_local,
        "end": end_local,
        "desc": desc,
    })

def fill_no_programming(programs, cid, day_start, day_end, block_minutes=120):
    cur = day_start
    while cur < day_end:
        nxt = min(cur + dt.timedelta(minutes=block_minutes), day_end)
        add_block(programs, cid, "No Programming Today.", cur, nxt)
        cur = nxt

def build_xmltv(processed, programs):
    out = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append('<tv generator-info-name="EPGOAT-Linker-FilenameMode">')
    # channels
    seen = set()
    for e in processed:
        cid = chan_id(e)
        if cid in seen: continue
        seen.add(cid)
        disp = e.tvg_name or e.display_name or cid
        out.append(f'  <channel id="{xml_esc(cid)}">')
        out.append(f'    <display-name lang="en">{xml_esc(disp)}</display-name>')
        if e.group_title:
            out.append(f'    <category lang="en">{xml_esc(e.group_title)}</category>')
        if e.tvg_logo:
            out.append(f'    <icon src="{xml_esc(e.tvg_logo)}" />')
        out.append('  </channel>')
    # programmes
    for e in processed:
        cid = chan_id(e)
        for prog in programs.get(cid, []):
            out.append(
                f'  <programme start="{fmt_xmltv_dt(prog["start"])}" stop="{fmt_xmltv_dt(prog["end"])}" channel="{xml_esc(cid)}">'
            )
            out.append(f'    <title lang="en">{xml_esc(prog["title"])}</title>')
            if prog.get("desc"):
                out.append(f'    <desc lang="en">{xml_esc(prog["desc"])}"</desc>')
            out.append('  </programme>')
    out.append('</tv>')
    return "\n".join(out)

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="EPGOAT filename-only EPG (APIs off, prefix filter)")
    ap.add_argument("--m3u", required=True, help="Path/URL to input M3U")
    ap.add_argument("--out-xmltv", required=True, help="XMLTV output path")
    ap.add_argument("--csv", help="Audit CSV path")
    ap.add_argument("--tz", default="America/Chicago", help="IANA timezone for schedule (e.g., America/Chicago)")
    ap.add_argument("--date", help="Target date YYYY-MM-DD (default: today in tz)")
    ap.add_argument("--event-duration-min", type=int, default=180, help="Event duration when time is known (minutes)")
    args = ap.parse_args()

    if ZoneInfo is None:
        print("ERROR: Python 3.9+ with zoneinfo required.", file=sys.stderr); sys.exit(2)
    try:
        central = ZoneInfo(args.tz)
    except Exception:
        print(f"ERROR: Invalid timezone '{args.tz}'", file=sys.stderr); sys.exit(2)

    now_central = dt.datetime.now(tz=central)
    if args.date:
        try:
            tgt_date = dt.date.fromisoformat(args.date)
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD", file=sys.stderr); sys.exit(2)
    else:
        tgt_date = now_central.date()

    day_start = dt.datetime(tgt_date.year, tgt_date.month, tgt_date.day, 0, 0, 0, tzinfo=central)
    day_end   = day_start + dt.timedelta(days=1)

    # Parse M3U
    entries = parse_m3u(args.m3u)

    # Filter by allowed prefixes
    processed = []
    matched_prefixes = []
    for e in entries:
        disp = (e.tvg_name or e.display_name or "").strip()
        ok, pref, _ = match_prefix_and_shell(disp)
        if ok:
            processed.append(e)
            matched_prefixes.append(pref)

    programs = {}
    # Build programs per channel
    for e, pref in zip(processed, matched_prefixes):
        cid = chan_id(e)
        disp = (e.tvg_name or e.display_name or cid).strip()

        classification, payload = classify_channel(disp, pref)

        if classification == "generic":
            fill_no_programming(programs, cid, day_start, day_end, block_minutes=120)
            continue

        # Event: try to parse a time; but time is optional
        # We'll try to find a time in payload. If missing, we do all-day "Airing Next (TBA)" (Q2=A).
        event_start_ct = try_parse_time(payload, year=tgt_date.year, central=central, date_context=tgt_date)

        if event_start_ct:
            # If the parsed time maps to a different date than target, show all-day "Airing Next @ ..."
            if event_start_ct.date() != tgt_date:
                airing = f"Airing Next: {payload.strip()} @ {event_start_ct.strftime('%b %d %I:%M %p')} CT"
                add_block(programs, cid, airing, day_start, day_end)
                continue

            # Today's event:
            # 1) Midnight -> Event start
            if event_start_ct > day_start:
                airing = f"Airing Next: {payload.strip()} @ {event_start_ct.strftime('%b %d %I:%M %p')} CT"
                add_block(programs, cid, airing, day_start, event_start_ct)

            # 2) Event block (default 180 mins)
            event_end = min(event_start_ct + dt.timedelta(minutes=args.event_duration_min), day_end)
            live_title = f"❗ {payload.strip()} ❗"
            add_block(programs, cid, live_title, event_start_ct, event_end)

            # 3) Remainder of day
            if event_end < day_end:
                fill_no_programming(programs, cid, event_end, day_end, block_minutes=120)

        else:
            # No time parsed → all-day "Airing Next (Time TBA)"
            # NOTE: Future enhancement: use API data to resolve TBA times when available.
            airing = f"Airing Next: {payload.strip()} (Time TBA)"
            add_block(programs, cid, airing, day_start, day_end)

    # Write XMLTV
    xmltv = build_xmltv(processed, programs)
    with open(args.out_xmltv, "w", encoding="utf-8") as f:
        f.write(xmltv)

    # Write CSV audit (processed-only)
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "tvg_id","tvg_name","display_name","group_title","tvg_logo","url",
                "channel_id","matched_prefix","classification","payload",
                "has_time","event_start_ct","event_duration_min","target_date"
            ])
            for e, pref in zip(processed, matched_prefixes):
                cid = chan_id(e)
                disp = (e.tvg_name or e.display_name or cid).strip()
                classification, payload = classify_channel(disp, pref)
                start_ct = try_parse_time(payload, year=tgt_date.year, central=ZoneInfo(args.tz), date_context=tgt_date) if classification=="event" else None
                w.writerow([
                    e.tvg_id, e.tvg_name, e.display_name, e.group_title, e.tvg_logo, e.url,
                    cid, pref, classification, payload.strip(),
                    bool(start_ct),
                    start_ct.strftime("%Y-%m-%d %I:%M %p %Z") if start_ct else "",
                    args.event_duration_min if start_ct else "",
                    tgt_date.isoformat(),
                ])

    print(f"[OK] XMLTV: {args.out_xmltv}")
    if args.csv: print(f"[OK] Audit CSV: {args.csv}")

if __name__ == "__main__":
    main()
