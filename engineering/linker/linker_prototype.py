#!/usr/bin/env python3
import argparse, re, sys, json, html, os
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
try:
    import yaml
except Exception:
    yaml = None

LOCAL_DEFAULT = "America/Chicago"
UTC = ZoneInfo("UTC")

def http_get_json(url, headers=None):
    try:
        req = Request(url, headers=headers or {"User-Agent":"EPGOAT-Linker/1.0"})
        with urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, ValueError) as e:
        print(f"[warn] http_get_json failed: {url} -> {e}", file=sys.stderr)
        return {}

def parse_m3u(path):
    out = []
    extinf = None
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("#EXTINF"):
                extinf = line
            elif line and not line.startswith("#"):
                url = line.strip()
                if extinf:
                    def _find_attr(s, key):
                        m = re.search(rf'{key}="([^"]+)"', s)
                        return m.group(1).strip() if m else None
                    tvg_id = _find_attr(extinf, "tvg-id")
                    tvg_name = _find_attr(extinf, "tvg-name")
                    group = _find_attr(extinf, "group-title")
                    title = extinf.split(",", 1)[1].strip() if "," in extinf else tvg_name or "Unknown"
                    logo = _find_attr(extinf, "tvg-logo")
                    out.append({"id": tvg_id or re.sub(r"[^A-Za-z0-9_]+","-", title).strip("-"),
                                "title": title, "group": group, "logo": logo, "url": url})
                extinf = None
    return out

def load_live_cfg(path):
    if yaml is None:
        raise SystemExit("PyYAML is required. Run: pip install pyyaml")
    with open(path, "r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    matchers = []
    for m in doc.get("matchers", []):
        if not m.get("enabled", True): continue
        try:
            pat = re.compile(m["id_pattern"], re.IGNORECASE)
        except Exception as e:
            print(f"[warn] bad regex in {m.get('name')}: {e}", file=sys.stderr); continue
        matchers.append({"name": m.get("name"), "pat": pat, "groups": [g.lower() for g in m.get("groups", [])]})
    return matchers

def split_vs(title):
    m = re.split(r"\s+vs\.?\s+|\s+v\.?\s+", title, flags=re.IGNORECASE)
    return m if len(m)==2 else None

def is_live_event_channel(ch, matchers):
    cid = (ch.get("id") or "").strip()
    grp = (ch.get("group") or "").lower()
    title = (ch.get("title") or "").lower()
    for m in matchers:
        if cid and m["pat"].match(cid):
            return True, m["name"]
    for m in matchers:
        if any(g in grp for g in m["groups"]):
            return True, m["name"]
    if split_vs(title):
        return True, "vs-fallback"
    return False, None

def infer_duration_minutes(title):
    t = title.lower()
    if any(k in t for k in ["mlb","nhl","nba","wnba","nfl","ncaaf","game","postseason","playoffs"]): return 180
    if any(k in t for k in ["uefa","premier","bundesliga","la liga","serie a"," vs "," v "]): return 120
    if any(k in t for k in ["show","pregame","postgame","media day","weekly","buzz","rundown"]): return 60
    return 120

def xmltv_dt(dt_utc): return dt_utc.strftime("%Y%m%d%H%M%S +0000")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--m3u", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--cfg", required=True)
    ap.add_argument("--date", default=date.today().isoformat())
    ap.add_argument("--tz", default=LOCAL_DEFAULT)
    args = ap.parse_args()

    local_tz = ZoneInfo(args.tz); the_day = date.fromisoformat(args.date)
    channels = parse_m3u(args.m3u)
    matchers = load_live_cfg(args.cfg)

    # Filter channels
    live_channels = []; counts = {}
    for ch in channels:
        ok, tag = is_live_event_channel(ch, matchers)
        if ok:
            live_channels.append(ch)
            counts[tag] = counts.get(tag, 0) + 1
    print("[info] live-event matches:", counts, file=sys.stderr)

    # Build simple pre/live/post blocks using heuristic start 7:30pm local (can be replaced by league API lookups)
    from datetime import datetime, timedelta
    programmes = {ch["id"]: [] for ch in live_channels}
    for ch in live_channels:
        ch_id = ch["id"]; title = ch["title"]
        sod_local = datetime(the_day.year,the_day.month,the_day.day,0,0,tzinfo=local_tz)
        start_local = datetime(the_day.year,the_day.month,the_day.day,19,30,tzinfo=local_tz)
        end_local = start_local + timedelta(minutes=infer_duration_minutes(title))
        eod_local = datetime(the_day.year,the_day.month,the_day.day,23,59,59,tzinfo=local_tz)

        # pre
        programmes[ch_id].append({"start": xmltv_dt(sod_local.astimezone(ZoneInfo("UTC"))),
                                  "stop": xmltv_dt(start_local.astimezone(ZoneInfo("UTC"))),
                                  "title": f"Next event: {start_local.strftime('%Y-%m-%d @ %-I:%M %p Central')}",
                                  "desc": "Upcoming live event on this channel."})
        # live
        programmes[ch_id].append({"start": xmltv_dt(start_local.astimezone(ZoneInfo("UTC"))),
                                  "stop": xmltv_dt(end_local.astimezone(ZoneInfo("UTC"))),
                                  "title": title, "category":"Sports",
                                  "desc": "Live event (heuristic).", "live": True})
        # post
        programmes[ch_id].append({"start": xmltv_dt(end_local.astimezone(ZoneInfo("UTC"))),
                                  "stop": xmltv_dt(eod_local.astimezone(ZoneInfo("UTC"))),
                                  "title": "No programming", "desc":"End of scheduled events for today."})

    parts = ['<?xml version="1.0" encoding="UTF-8"?>','<tv generator-info-name="EPGOAT-Linker" generator-info-url="https://example.org">']
    for ch in live_channels:
        cid = ch["id"]; parts.append(f'  <channel id="{html.escape(cid)}">')
        parts.append(f'    <display-name>{html.escape(ch["title"])}</display-name>')
        if ch.get("logo"): parts.append(f'    <icon src="{html.escape(ch["logo"])}" />')
        parts.append('  </channel>')
    for cid, plist in programmes.items():
        for p in plist:
            parts.append(f'  <programme start="{p["start"]}" stop="{p["stop"]}" channel="{html.escape(cid)}">')
            parts.append(f'    <title>{html.escape(p["title"])}')
            parts.append('</title>')
            if p.get("category"): parts.append(f'    <category>{html.escape(p["category"])}</category>')
            if p.get("desc"): parts.append(f'    <desc>{html.escape(p["desc"])}</desc>')
            if p.get("live"): parts.append('    <live/>')
            parts.append('  </programme>')
    parts.append('</tv>')
    with open(args.out, "w", encoding="utf-8") as fh: fh.write("\n".join(parts))
    print(f"Wrote XMLTV: {args.out} (channels={len(live_channels)})")

if __name__ == "__main__":
    main()
