#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EPGOAT Linker Prototype — Pattern-based, API-OFF, filename-driven scheduling

- Filters channels by pattern matching on numbered channel families (e.g., "ESPN+ 01:", "NHL 15:").
- No external APIs (by design). Event timing is parsed from the channel name if present.
- Event detection = "payload beyond the shell":
    Shell = <FAMILY> + <NUMBER> + <DELIMITER>.
    If anything meaningful remains after removing the shell, it's an Event (unless it's in a fluff ignore-list).
- Special-case: "Peacock 01: Studio" is treated as GENERIC per your instruction; otherwise "Studio/Pregame/Postgame"
  payloads are treated as Event (Q3).

Scheduling for a target DATE (default: today in America/Chicago):
  * GENERIC channel (no meaningful payload): fill the day with 2-hour "No Programming Today." blocks.
  * EVENT channel:
      - If a time is parsed AND it's on the target date:
            00:00 → event_start      : "Airing Next: <Title> @ <... CT>"
            event_start → +180 mins  : "● <Title> ●"
            remainder of day         : 2-hour "No Programming Today."
      - If a time is parsed BUT it's NOT on the target date (Q5=A):
            Whole day: "Airing Next: <Title> @ <... CT>"
      - If NO time is parsed (Q2=A):
            Whole day: "Airing Next: <Title> (Time TBA)"
            NOTE: In the future, we'll restore API lookups to resolve exact times when TBA.

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
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

# ----------------------------
# Allowed channel patterns (pattern-based matching for numbered channels)
# These patterns match specific numbered channel families instead of just prefixes
# ----------------------------
ALLOWED_CHANNEL_PATTERNS = [
    # Major Sports Leagues
    (r'^BIG10\+ \d{2}:', 'BIG10+'),
    (r'^Bundesliga \d{2}:', 'Bundesliga'),
    (r'^EPL \d{2}:?', 'EPL'),  # Colon optional
    (r'^EPL\d{2}', 'EPL'),  # No space variant
    (r'^La Liga \d{2}:', 'La Liga'),
    (r'^Ligue1 \d{2}:', 'Ligue1'),
    (r'^Serie A \d{2}:', 'Serie A'),
    (r'^Scottish Premiership \d{2}:', 'Scottish Premiership'),
    (r'^SPFL \d{2}:', 'SPFL'),

    # Basketball Leagues
    (r'^NBA \d{2}:', 'NBA'),
    (r'^NCAAB \d{2}:', 'NCAAB'),
    (r'^NCAAW B \d{2}:', 'NCAAW B'),
    (r'^NJCAA Men\'s Basketball \d{2}:', 'NJCAA Men\'s Basketball'),
    (r'^NJCAA Women\'s Basketball \d{2}:', 'NJCAA Women\'s Basketball'),
    (r'^USA Real NBA \d{2}:', 'USA Real NBA'),
    (r'^WNBA \d{2}:?', 'WNBA'),  # Colon optional
    (r'^FIBA \d{2}:', 'FIBA'),

    # Football (American)
    (r'^NCAAF \d{2,3}:?', 'NCAAF'),  # Colon optional (no space before colon due to normalization)
    (r'^NFL \d{2}:?', 'NFL'),  # Colon optional
    (r'^NFL Game Pass \d+', 'NFL Game Pass'),
    (r'^NFL Multi Screen / HDR \d+', 'NFL Multi Screen'),
    (r'^NFL\s+\|\s+\d{2}', 'NFL |'),

    # Hockey Leagues
    (r'^NHL \d{2}:', 'NHL'),
    (r'^NHL \| \d{2}:', 'NHL |'),
    (r'^USA Real NHL \d{2}:', 'USA Real NHL'),
    (r'^WHL \d{2}:', 'WHL'),
    (r'^QMJHL \d{2}:', 'QMJHL'),
    (r'^OHL \d{2}:', 'OHL'),

    # Baseball Leagues
    (r'^MLB \d{2}:', 'MLB'),
    (r'^MiLB \d{2}:', 'MiLB'),
    (r'^MILB \d{2}:', 'MILB'),
    (r'^USA Real MLB \d{2}:', 'USA Real MLB'),

    # Soccer/Football (International)
    (r'^MLS \d{2}:?', 'MLS'),  # Colon optional (no space before colon due to normalization)
    (r'^MLS \d{1,3} \|', 'MLS'),  # MLS with pipe separator
    (r'^MLS NEXT PRO \d{2}', 'MLS NEXT PRO'),
    (r'^MLS Espanolⓧ \d{2}', 'MLS Espanol'),
    (r'^USA \| MLS \d{2}', 'USA | MLS'),
    (r'^USA Soccer\d{2}:', 'USA Soccer'),
    (r'^FA Cup \d{2}', 'FA Cup'),
    (r'^EFL\d{2}', 'EFL'),
    (r'^Super League \+ \d{2}', 'Super League'),
    (r'^UEFA Champions League \d{2}:', 'UEFA Champions League'),
    (r'^UEFA Europa League \d{2}:', 'UEFA Europa League'),
    (r'^UEFA Europa Conf\. League \d{2}:', 'UEFA Europa Conf League'),
    (r'^UEFA/FIFA \d{2}', 'UEFA/FIFA'),
    (r'^GAAGO:GAME \d{2}', 'GAAGO'),  # No spaces around colon due to normalization
    (r'^LOI GAME \d{2}', 'LOI'),
    (r'^National League TV \d{2}', 'National League TV'),

    # Streaming Services
    (r'^DAZN BE \d{2}:', 'DAZN BE'),
    (r'^DAZN CA \d+:?', 'DAZN CA'),  # No leading zeros, colon optional
    (r'^ESPN\+ \d+:?', 'ESPN+'),  # Colon optional
    (r'^Fanatiz \d{2}:', 'Fanatiz'),
    (r'^Flo Football \d{2}:', 'Flo Football'),
    (r'^Flo Racing \d{2}:', 'Flo Racing'),
    (r'^Flo Sports \d{2,4}:', 'Flo Sports'),
    (r'^Paramount\+ \d{2,3}:?', 'Paramount+'),  # Colon optional (no space before colon)
    (r'^Peacock \d{2}:', 'Peacock'),
    (r'^Prime US \d{2}:', 'Prime US'),
    (r'^SEC\+ / ACC extra \d{2}', 'SEC+/ACC extra'),
    (r'^Fubo Sports Network \d{2}', 'Fubo Sports Network'),
    (r'^Sportsnet\+ \d{2}:?', 'Sportsnet+'),  # Colon optional
    (r'^TSN\+ \d{2}:', 'TSN+'),

    # International Streaming
    (r'^MAX NL \d{2,3}:', 'MAX NL'),
    (r'^MAX SE \d{2,3}:', 'MAX SE'),
    (r'^MAX USA \d{2}:', 'MAX USA'),
    (r'^Viaplay NL \d{2}:', 'Viaplay NL'),
    (r'^Viaplay SE \d{2}:', 'Viaplay SE'),
    (r'^Viaplay NO \d+', 'Viaplay NO'),  # No colon
    (r'^TV2 NO \d{2}:', 'TV2 NO'),
    (r'^Tv4 Play SE \d{2}:', 'Tv4 Play SE'),
    (r'^Sky Sports\+ \|', 'Sky Sports+'),
    (r'^Sky Tennis\+ \|', 'Sky Tennis+'),
    (r'^Setanta Sports \d{2}:', 'Setanta Sports'),

    # Tennis and Combat Sports
    (r'^Tennis \d{2}:', 'Tennis'),
    (r'^Tennis TV \| Event \d{2}', 'Tennis TV'),
    (r'^UFC \d{2}', 'UFC'),
    (r'^TrillerTV Event \d{2}', 'TrillerTV'),
    (r'^Matchroom Event \d+', 'Matchroom'),

    # Other Sports
    (r'^LIVE EVENT \d{2}', 'LIVE EVENT'),
    (r'^Dirtvision:EVENT \d{2}', 'Dirtvision'),  # No spaces around colon due to normalization
    (r'^Clubber \d{2}', 'Clubber'),
    (r'^NCAA Softball \d{2}:', 'NCAA Softball')
]

# ----------------------------
# Pattern regex compilation (module-level for performance)
# We compile these once at import time to avoid repeated regex compilation
# ----------------------------
def build_channel_regex(pattern: str) -> re.Pattern:
    """
    Build a regex pattern for numbered channel matching.
    The pattern already includes the numbering format, so we just add
    optional trailing content after the channel identifier.
    """
    # Add trailing pattern to match everything after the channel identifier
    full_pattern = pattern + r'\s*'
    return re.compile(full_pattern, re.IGNORECASE | re.UNICODE)

CHANNEL_PATTERNS = [(name, build_channel_regex(pattern)) for pattern, name in ALLOWED_CHANNEL_PATTERNS]

def match_prefix_and_shell(name: str) -> tuple[bool, str | None, re.Match | None]:
    """
    Check if name matches an allowed channel pattern.
    Returns (matched, channel_family_name, regex_match_object)
    """
    n = (name or "").strip()
    # allow “  : ” or “ :” before the colon
    n = re.sub(r"\s+:\s*", ":", n)
    for family_name, rx in CHANNEL_PATTERNS:
        m = rx.match(n)
        if m:
            return True, family_name, m
    return False, None, None

# ----------------------------
# Fluff/Ignore payload tokens (treated as GENERIC if they're all you see)
# These are technical terms that don't indicate actual event content
# ----------------------------
FLUFF_TOKENS = {
    "LIVE","TEST","FHD","UHD","HEVC","EN","ES","ALT","MULTI","BACKUP","FEED","EVENT","STREAM",
    "1080P","2160P","4K","HDR","SD","HD","H264","H265","AAC","AC3","EAC3",
}

# Special-case overrides (prefix -> set of exact payloads considered GENERIC)
# Currently empty per instructions
SPECIAL_GENERIC_EXCEPTIONS = {
    # "Peacock": {"STUDIO"},  # Removed per instruction
}

# ----------------------------
# Time parsing with improved error handling
# ----------------------------
EVENT_TIME_RXES = [
    # "Oct 09 08:55 AM ET" or "Oct 9 8:55pm CT"  (month + day + time + AM/PM + TZ)
    re.compile(r"([A-Za-z]{3})\s+(\d{1,2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "07:30 PM ET"  (time + AM/PM + TZ; no date)
    re.compile(r"(\d{1,2}):(\d{2})\s*(AM|PM)\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "10/09 20:00 CET"  (MM/DD + 24h time + TZ)
    re.compile(r"(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "20:00 ET"  (24h time + TZ; no date)
    re.compile(r"(\d{1,2}):(\d{2})\s*([A-Za-z]{1,4})", re.IGNORECASE),

    # "8pm PT"  (hour + AM/PM + TZ; no minutes, no date)
    re.compile(r"(\d{1,2})\s*(AM|PM)\s*([A-Za-z]{1,4})", re.IGNORECASE),
]

# Case-insensitive month mapping
MONTHS = {m.lower(): i for i, m in enumerate(
    ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"], start=1
)}

TZ_TO_IANA = {
    # North America
    "ET":"America/New_York","EST":"America/New_York","EDT":"America/New_York",
    "CT":"America/Chicago","CST":"America/Chicago","CDT":"America/Chicago",
    "MT":"America/Denver","MST":"America/Denver","MDT":"America/Denver",
    "PT":"America/Los_Angeles","PST":"America/Los_Angeles","PDT":"America/Los_Angeles",
    "UTC":"UTC","GMT":"UTC",
    # Europe
    "CET":"Europe/Berlin","CEST":"Europe/Berlin",
    "BST":"Europe/London","WEST":"Europe/Lisbon","WET":"Europe/Lisbon",
}

def _tzinfo_for_abbr(abbr: str) -> ZoneInfo:
    """Get ZoneInfo for timezone abbreviation with fallback to ET."""
    key = (abbr or "").upper()
    tzname = TZ_TO_IANA.get(key)
    if not tzname:
        print(f"[warn] Unrecognized timezone abbreviation '{abbr}', defaulting to ET (America/New_York).", file=sys.stderr)
        tzname = "America/New_York"
    return ZoneInfo(tzname)

def _fix_12hour_time(hour: int, ampm: str) -> int:
    """
    Convert 12-hour time to 24-hour format correctly.
    Fixes the bug where 12:30 PM becomes 0:30 PM.
    """
    h = int(hour)
    is_pm = ampm.upper() == "PM"
    
    if h == 12:
        # 12:xx AM -> 00:xx (midnight hour)
        # 12:xx PM -> 12:xx (noon hour)
        return 12 if is_pm else 0
    else:
        # 1-11 AM -> 1-11
        # 1-11 PM -> 13-23
        return h + 12 if is_pm else h

def _handle_year_rollover(parsed_dt: dt.datetime, date_context: dt.date) -> dt.datetime:
    """
    Handle year rollover for dates parsed without explicit years.
    If parsed date is more than 60 days in the past, assume it's next year.
    Example: In December 2025, "Jan 15" likely means Jan 15, 2026.
    """
    days_diff = (date_context - parsed_dt.date()).days
    
    if days_diff > 60:  # More than 2 months in the past
        parsed_dt = parsed_dt.replace(year=parsed_dt.year + 1)
        print(f"[info] Adjusted year rollover: {parsed_dt.strftime('%Y-%m-%d %H:%M %Z')}", file=sys.stderr)
    
    return parsed_dt

def try_parse_time(payload: str, year: int, central: ZoneInfo, date_context: dt.date) -> dt.datetime | None:
    """
    Try to find a time in the payload; normalize to Central if found.
    For patterns without an explicit date, we anchor to the provided date_context (the --date).
    
    Returns None if no valid time pattern is found.
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
                mon = MONTHS.get(mon_abbr[:3].lower())  # Case-insensitive
                if not mon:
                    continue
                
                hour = _fix_12hour_time(int(hh), ampm)
                minute = int(mm)
                src = _tzinfo_for_abbr(tz_abbr)
                local = dt.datetime(year, mon, int(day), hour, minute, 0, tzinfo=src)
                local = _handle_year_rollover(local, date_context)
                return local.astimezone(central)

            elif rx is EVENT_TIME_RXES[1]:
                # "07:30 PM ET" (no date → use date_context)
                hh, mm, ampm, tz_abbr = m.groups()
                hour = _fix_12hour_time(int(hh), ampm)
                minute = int(mm)
                src = _tzinfo_for_abbr(tz_abbr)
                local = dt.datetime(date_context.year, date_context.month, date_context.day, hour, minute, 0, tzinfo=src)
                return local.astimezone(central)

            elif rx is EVENT_TIME_RXES[2]:
                # "10/09 20:00 CET"
                mon, day, hh, mm, tz_abbr = m.groups()
                hour = int(hh)
                minute = int(mm)
                src = _tzinfo_for_abbr(tz_abbr)
                local = dt.datetime(year, int(mon), int(day), hour, minute, 0, tzinfo=src)
                local = _handle_year_rollover(local, date_context)
                return local.astimezone(central)

            elif rx is EVENT_TIME_RXES[3]:
                # "20:00 ET" (no date → use date_context)
                hh, mm, tz_abbr = m.groups()
                hour = int(hh)
                minute = int(mm)
                src = _tzinfo_for_abbr(tz_abbr)
                local = dt.datetime(date_context.year, date_context.month, date_context.day, hour, minute, 0, tzinfo=src)
                return local.astimezone(central)

            else:
                # "8pm PT" (no minutes, no date → use date_context)
                hh, ampm, tz_abbr = m.groups()
                hour = _fix_12hour_time(int(hh), ampm)
                src = _tzinfo_for_abbr(tz_abbr)
                local = dt.datetime(date_context.year, date_context.month, date_context.day, hour, 0, 0, tzinfo=src)
                return local.astimezone(central)

        except (ValueError, OverflowError) as ex:
            print(f"[warn] Failed to parse time from '{text}' using pattern {EVENT_TIME_RXES.index(rx)}: {ex}", file=sys.stderr)
            continue
        except Exception as ex:
            print(f"[warn] Unexpected error parsing time from '{text}': {ex}", file=sys.stderr)
            continue

    return None


# ----------------------------
# XML helpers
# ----------------------------
def xml_esc(s: str) -> str:
    """Escape string for XML output."""
    return saxutils.escape(s or "", {'"': "&quot;"})

def chan_id(entry) -> str:
    """
    Generate collision-safe channel ID.
    Uses display/tvg name + stable URL hash token.
    """
    base = (entry.tvg_id or entry.tvg_name or entry.display_name or "channel").strip()
    base = re.sub(r"\s+", "_", base)
    tok = hashlib.sha1(entry.url.encode("utf-8")).hexdigest()[:8]
    return f"{base}__{tok}"

def fmt_xmltv_dt(dt_obj: dt.datetime) -> str:
    """Format datetime for XMLTV (UTC with +0000 offset)."""
    return dt_obj.astimezone(dt.timezone.utc).strftime("%Y%m%d%H%M%S +0000")

def validate_url(url: str) -> bool:
    """Basic URL validation."""
    if not url or not url.strip():
        return False
    url = url.strip()
    # Check for common URL patterns
    if not (url.startswith("http://") or url.startswith("https://") or url.startswith("rtmp://") or url.startswith("rtsp://")):
        return False
    return True

def is_vod_url(url: str) -> bool:
    """Check if URL is VOD (movie or series) content."""
    url_lower = url.lower()
    return '/movie/' in url_lower or '/series/' in url_lower

# ----------------------------
# M3U parsing
# ----------------------------
class M3UEntry:
    """Represents a single M3U playlist entry."""
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
    """Parse attributes from #EXTINF line."""
    if not info_line.startswith("#EXTINF"):
        return {}, info_line.strip()
    try:
        header, disp = info_line.split(",", 1)
    except ValueError:
        header, disp = info_line, ""
    attrs = {k.lower(): v for k, v in re.findall(r'([\w\-]+)="([^"]*)"', header)}
    return attrs, disp.strip()

def parse_m3u(path: str):
    """
    Parse M3U file or URL.
    Returns list of M3UEntry objects (Live TV only, excludes VOD).
    """
    lines = []
    if path.startswith("http://") or path.startswith("https://"):
        import urllib.request
        with urllib.request.urlopen(path) as resp:
            lines = resp.read().decode("utf-8", "replace").splitlines()
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.read().splitlines()

    entries = []
    vod_count = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("#EXTINF"):
            attrs, disp = parse_extinf_attrs(line)
            url, j = "", i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                j += 1
                if not nxt or nxt.startswith("#"):
                    continue
                url = nxt
                break
            if url and validate_url(url):
                # Skip VOD content (movies and series)
                if is_vod_url(url):
                    vod_count += 1
                else:
                    entries.append(M3UEntry(attrs, disp, url))
            elif url:
                print(f"[warn] Skipping entry with invalid URL: {url[:50]}...", file=sys.stderr)
            i = j
        else:
            i += 1

    if vod_count > 0:
        print(f"[info] Filtered out {vod_count} VOD entries (movies/series)", file=sys.stderr)

    return entries

# ----------------------------
# Classification helpers with diagnostics
# ----------------------------
class ChannelClassification:
    """Holds classification result with diagnostic info."""
    def __init__(self, classification: str, payload: str, shell_end: int, tokens: list[str], warnings: list[str]):
        self.classification = classification
        self.payload = payload
        self.shell_end = shell_end
        self.tokens = tokens
        self.warnings = warnings

def classify_channel(name: str, matched_family: str, match_obj: re.Match | None) -> ChannelClassification:
    """
    Classify channel as "generic" or "event" based on payload.

    Returns ChannelClassification with diagnostic information.
    """
    warnings = []

    if not match_obj:
        # Shouldn't happen if called correctly, but handle gracefully
        warnings.append("no_pattern_match")
        return ChannelClassification("generic", "", 0, [], warnings)

    payload = name[match_obj.end():].strip()
    shell_end = match_obj.end()

    if not payload:
        warnings.append("empty_payload")
        return ChannelClassification("generic", "", shell_end, [], warnings)

    # Normalize payload for token analysis
    payload_upper = re.sub(r"\s+", " ", payload).strip().upper()

    # Check special-case generic exceptions
    if matched_family and matched_family in SPECIAL_GENERIC_EXCEPTIONS:
        if payload_upper in SPECIAL_GENERIC_EXCEPTIONS[matched_family]:
            warnings.append("special_generic_exception")
            return ChannelClassification("generic", payload, shell_end, [], warnings)

    # Extract tokens for analysis
    tokens = [t for t in re.split(r"[^A-Z0-9]+", payload_upper) if t]

    # If payload consists only of fluff tokens, treat as generic
    if tokens and all(t in FLUFF_TOKENS for t in tokens):
        warnings.append("all_fluff_tokens")
        return ChannelClassification("generic", payload, shell_end, tokens, warnings)

    # Check for ambiguous case: family only with no distinguishing payload
    if matched_family and payload_upper == matched_family.upper():
        warnings.append("ambiguous_family_only")

    # Otherwise, it's an event (even without a parseable time)
    return ChannelClassification("event", payload, shell_end, tokens, warnings)

# ----------------------------
# Programme building
# ----------------------------
def add_block(programs, cid, title, start_local, end_local, desc=""):
    """Add a programme block to the schedule."""
    programs.setdefault(cid, []).append({
        "title": title,
        "start": start_local,
        "end": end_local,
        "desc": desc,
    })

def fill_no_programming(programs, cid, day_start, day_end, block_minutes=120):
    """Fill time range with 'No Programming Today' blocks."""
    cur = day_start
    while cur < day_end:
        nxt = min(cur + dt.timedelta(minutes=block_minutes), day_end)
        add_block(programs, cid, "No Programming Today.", cur, nxt)
        cur = nxt

def validate_schedule(programs, cid):
    """
    Validate programme schedule for a channel.
    Checks for overlaps and excessive durations.
    """
    progs = programs.get(cid, [])
    if not progs:
        return
    
    sorted_progs = sorted(progs, key=lambda p: p["start"])
    
    for i in range(len(sorted_progs) - 1):
        current = sorted_progs[i]
        next_prog = sorted_progs[i + 1]
        
        # Check for overlap
        if current["end"] > next_prog["start"]:
            print(f"[warn] Channel {cid}: Programme overlap detected", file=sys.stderr)
        
        # Check for excessive duration (>12 hours)
        duration = (current["end"] - current["start"]).total_seconds() / 3600
        if duration > 12:
            print(f"[warn] Channel {cid}: Programme duration exceeds 12 hours ({duration:.1f}h)", file=sys.stderr)

def build_xmltv(processed, programs, tz_name: str, target_date: dt.date):
    """
    Build XMLTV document from processed entries and programmes.
    Returns XML string.
    """
    out = []
    out.append('<?xml version="1.0" encoding="UTF-8"?>')
    out.append(f'<!-- Generated by EPGOAT-Linker-FilenameMode -->')
    out.append(f'<!-- Target date: {target_date.isoformat()} ({tz_name}) -->')
    out.append(f'<!-- All timestamps in UTC -->')
    out.append('<tv generator-info-name="EPGOAT-Linker-FilenameMode">')
    
    # Write channels
    seen = set()
    for e in processed:
        cid = chan_id(e)
        if cid in seen:
            continue
        seen.add(cid)
        
        # Prefer tvg_name, then display_name, then tvg_id, then cid
        disp = e.tvg_name or e.display_name or e.tvg_id or cid
        out.append(f'  <channel id="{xml_esc(cid)}">')
        out.append(f'    <display-name lang="en">{xml_esc(disp)}</display-name>')
        if e.group_title:
            out.append(f'    <category lang="en">{xml_esc(e.group_title)}</category>')
        if e.tvg_logo:
            out.append(f'    <icon src="{xml_esc(e.tvg_logo)}" />')
        out.append('  </channel>')
    
    # Write programmes (sorted by start time for each channel)
    for e in processed:
        cid = chan_id(e)
        progs = programs.get(cid, [])
        progs_sorted = sorted(progs, key=lambda p: p["start"])
        
        for prog in progs_sorted:
            out.append(
                f'  <programme start="{fmt_xmltv_dt(prog["start"])}" '
                f'stop="{fmt_xmltv_dt(prog["end"])}" channel="{xml_esc(cid)}">'
            )
            out.append(f'    <title lang="en">{xml_esc(prog["title"])}</title>')
            if prog.get("desc"):
                out.append(f'    <desc lang="en">{xml_esc(prog["desc"])}</desc>')
            out.append('  </programme>')
    
    out.append('</tv>')
    return "\n".join(out)

# ----------------------------
# Deduplication
# ----------------------------
def deduplicate_entries(entries: list[M3UEntry]) -> tuple[list[M3UEntry], dict]:
    """
    Remove duplicate entries based on URL.
    Returns (deduplicated_list, stats_dict).
    """
    seen_urls = {}
    deduplicated = []
    duplicates = 0
    
    for e in entries:
        url_key = e.url.strip().lower()
        if url_key in seen_urls:
            duplicates += 1
            print(f"[warn] Duplicate URL found, skipping: {e.display_name or e.tvg_name}", file=sys.stderr)
        else:
            seen_urls[url_key] = True
            deduplicated.append(e)
    
    return deduplicated, {"duplicates_removed": duplicates}

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="EPGOAT filename-only EPG (APIs off, pattern-based channel filter)")
    ap.add_argument("--m3u", required=True, help="Path/URL to input M3U")
    ap.add_argument("--out-xmltv", required=True, help="XMLTV output path")
    ap.add_argument("--csv", help="Audit CSV path")
    ap.add_argument("--tz", default="America/Chicago", help="IANA timezone for schedule (e.g., America/Chicago)")
    ap.add_argument("--date", help="Target date YYYY-MM-DD (default: today in tz)")
    ap.add_argument("--event-duration-min", type=int, default=180, help="Event duration when time is known (minutes)")
    ap.add_argument("--max-event-duration-min", type=int, default=360, help="Maximum event duration (minutes)")
    args = ap.parse_args()

    # Validate Python version and timezone support
    if ZoneInfo is None:
        print("ERROR: Python 3.9+ with zoneinfo required.", file=sys.stderr)
        sys.exit(2)
    
    try:
        central = ZoneInfo(args.tz)
    except Exception as ex:
        print(f"ERROR: Invalid timezone '{args.tz}': {ex}", file=sys.stderr)
        sys.exit(2)

    # Determine target date
    now_central = dt.datetime.now(tz=central)
    if args.date:
        try:
            tgt_date = dt.date.fromisoformat(args.date)
        except ValueError:
            print("ERROR: --date must be YYYY-MM-DD", file=sys.stderr)
            sys.exit(2)
    else:
        tgt_date = now_central.date()

    day_start = dt.datetime(tgt_date.year, tgt_date.month, tgt_date.day, 0, 0, 0, tzinfo=central)
    day_end = day_start + dt.timedelta(days=1)

    print(f"[info] Processing M3U: {args.m3u}", file=sys.stderr)
    print(f"[info] Target date: {tgt_date.isoformat()} ({args.tz})", file=sys.stderr)

    # Parse M3U
    try:
        entries = parse_m3u(args.m3u)
        print(f"[info] Found {len(entries)} live TV entries in M3U (VOD excluded)", file=sys.stderr)
    except Exception as ex:
        print(f"ERROR: Failed to parse M3U: {ex}", file=sys.stderr)
        sys.exit(1)

    # Deduplicate
    entries, dedup_stats = deduplicate_entries(entries)
    if dedup_stats["duplicates_removed"] > 0:
        print(f"[info] Removed {dedup_stats['duplicates_removed']} duplicate URLs", file=sys.stderr)

    # Filter by allowed channel patterns and cache match results for performance
    processed = []
    match_data = []  # Store (family_name, match_obj) tuples

    for e in entries:
        disp = (e.tvg_name or e.display_name or "").strip()
        ok, family_name, match_obj = match_prefix_and_shell(disp)
        if ok:
            processed.append(e)
            match_data.append((family_name, match_obj))

    print(f"[info] Filtered to {len(processed)} channels matching allowed channel patterns", file=sys.stderr)

    # Build programmes for each channel
    programs = {}
    stats = {
        "generic": 0,
        "event": 0,
        "event_with_time": 0,
        "event_tba": 0,
        "event_wrong_date": 0,
        "ambiguous": 0,
    }
    
    # Store classifications for CSV output
    classifications = []

    for e, (family_name, match_obj) in zip(processed, match_data):
        cid = chan_id(e)
        disp = (e.tvg_name or e.display_name or e.tvg_id or cid).strip()

        # Classify using cached match object
        classif = classify_channel(disp, family_name, match_obj)
        classifications.append(classif)

        if "ambiguous_family_only" in classif.warnings:
            stats["ambiguous"] += 1

        if classif.classification == "generic":
            stats["generic"] += 1
            fill_no_programming(programs, cid, day_start, day_end, block_minutes=120)
            continue

        stats["event"] += 1

        # Try to parse event time
        event_start_ct = try_parse_time(classif.payload, year=tgt_date.year, central=central, date_context=tgt_date)

        if event_start_ct:
            # Event has a parseable time
            if event_start_ct.date() != tgt_date:
                # Event is on a different date - show all-day "Airing Next"
                stats["event_wrong_date"] += 1
                airing = f"Airing Next: {classif.payload.strip()} @ {event_start_ct.strftime('%b %d %I:%M %p')} CT"
                add_block(programs, cid, airing, day_start, day_end)
                continue

            # Event is today
            stats["event_with_time"] += 1
            
            # Pre-event block (midnight to event start)
            if event_start_ct > day_start:
                airing = f"Airing Next: {classif.payload.strip()} @ {event_start_ct.strftime('%b %d %I:%M %p')} CT"
                add_block(programs, cid, airing, day_start, event_start_ct)

            # Event block (cap at max duration and end of day)
            event_duration = min(args.event_duration_min, args.max_event_duration_min)
            event_end = min(
                event_start_ct + dt.timedelta(minutes=event_duration),
                day_end
            )
            live_title = f"● {classif.payload.strip()} ●"
            add_block(programs, cid, live_title, event_start_ct, event_end)

            # Post-event filler
            if event_end < day_end:
                fill_no_programming(programs, cid, event_end, day_end, block_minutes=120)

        else:
            # No parseable time - all-day "Airing Next (Time TBA)"
            stats["event_tba"] += 1
            airing = f"Airing Next: {classif.payload.strip()} (Time TBA)"
            add_block(programs, cid, airing, day_start, day_end)
            # NOTE: Future enhancement - API integration would resolve TBA times here

    # Validate schedules
    for e in processed:
        validate_schedule(programs, chan_id(e))

    # Print statistics summary
    print("\n" + "="*60, file=sys.stderr)
    print("PROCESSING SUMMARY (Live TV Only)", file=sys.stderr)
    print("="*60, file=sys.stderr)
    print(f"Live TV entries found:       {len(entries) + dedup_stats['duplicates_removed']}", file=sys.stderr)
    print(f"Duplicates removed:          {dedup_stats['duplicates_removed']}", file=sys.stderr)
    print(f"After deduplication:         {len(entries)}", file=sys.stderr)
    print(f"Matched channel patterns:    {len(processed)}", file=sys.stderr)
    print(f"", file=sys.stderr)
    print(f"Channel Classifications:", file=sys.stderr)
    print(f"  Generic channels:          {stats['generic']}", file=sys.stderr)
    print(f"  Event channels:            {stats['event']}", file=sys.stderr)
    print(f"    - With parsed time:      {stats['event_with_time']}", file=sys.stderr)
    print(f"    - Time TBA:              {stats['event_tba']}", file=sys.stderr)
    print(f"    - Wrong date:            {stats['event_wrong_date']}", file=sys.stderr)
    if stats['ambiguous'] > 0:
        print(f"  Ambiguous (prefix only):   {stats['ambiguous']}", file=sys.stderr)
    print("="*60 + "\n", file=sys.stderr)

    # Write XMLTV
    try:
        xmltv = build_xmltv(processed, programs, args.tz, tgt_date)
        with open(args.out_xmltv, "w", encoding="utf-8") as f:
            f.write(xmltv)
        print(f"[OK] XMLTV written: {args.out_xmltv}", file=sys.stderr)
    except Exception as ex:
        print(f"ERROR: Failed to write XMLTV: {ex}", file=sys.stderr)
        sys.exit(1)

    # Write CSV audit with enhanced diagnostics
    if args.csv:
        try:
            with open(args.csv, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "tvg_id", "tvg_name", "display_name", "group_title", "tvg_logo", "url",
                    "channel_id", "matched_family", "classification", "payload",
                    "shell_end_position", "tokens_found", "parse_warnings",
                    "has_time", "event_start_ct", "event_duration_min", "target_date"
                ])
                for e, (family_name, _), classif in zip(processed, match_data, classifications):
                    cid = chan_id(e)
                    disp = (e.tvg_name or e.display_name or e.tvg_id or cid).strip()

                    # Try to parse time for events
                    start_ct = None
                    if classif.classification == "event":
                        start_ct = try_parse_time(classif.payload, year=tgt_date.year, central=central, date_context=tgt_date)

                    w.writerow([
                        e.tvg_id, e.tvg_name, e.display_name, e.group_title, e.tvg_logo, e.url,
                        cid, family_name, classif.classification, classif.payload.strip(),
                        classif.shell_end,
                        ",".join(classif.tokens) if classif.tokens else "",
                        ",".join(classif.warnings) if classif.warnings else "",
                        bool(start_ct),
                        start_ct.strftime("%Y-%m-%d %I:%M %p %Z") if start_ct else "",
                        args.event_duration_min if start_ct else "",
                        tgt_date.isoformat(),
                    ])
            print(f"[OK] Audit CSV written: {args.csv}", file=sys.stderr)
        except Exception as ex:
            print(f"ERROR: Failed to write CSV: {ex}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()
