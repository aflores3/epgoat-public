#!/usr/bin/env python3
"""
Verify channel list comprehensiveness in M3U file.
Usage: python verify_channels.py <path_to_m3u_file>
"""

import sys
import re
from collections import defaultdict

# Known channel families we've identified
KNOWN_FAMILIES = [
    r'^BIG10\+ \d+:',
    r'^Bundesliga \d+:',
    r'^DAZN BE \d+:',
    r'^DAZN CA \d+:',
    r'^EPL \d+:',
    r'^ESPN\+ \d+',
    r'^Fanatiz \d+:',
    r'^Flo Football \d+:',
    r'^Flo Racing \d+:',
    r'^Flo Sports \d+:',
    r'^La Liga \d+:',
    r'^Ligue1 \d+:',
    r'^LIVE EVENT \d+:',
    r'^MAX NL \d+:',
    r'^MAX SE \d+:',
    r'^MAX USA \d+:',
    r'^MLB \d+:',
    r'^MLS \d+:',
    r'^NBA \d+:',
    r'^NCAAF \d+:',
    r'^NFL \d+:',
    r'^NHL \d+:',
    r'^NHL \| \d+:',
    r'^Paramount\+ \d+:',
    r'^Peacock \d+:',
    r'^Serie A \d+:',
    r'^Sportsnet\+ \d+:',
    r'^Tennis \d+:',
    r'^TSN\+ \d+:',
    r'^UEFA Champions League \d+:',
    r'^UEFA Europa League \d+:',
    r'^Viaplay NL \d+:',
    r'^Viaplay SE \d+:',

    # Streaming Services - Sports Events
    r'^Prime US \d+:',           # Prime Video sports events
    r'^Paramount\+ \d+ :',       # Paramount+ numbered channels (with space before colon)
    r'^Amazon US:',              # Amazon streaming channels

    # Sports Leagues (additional variations)
    r'^MiLB \d+:',              # Minor League Baseball (uppercase i)
    r'^MILB \d+:',              # Minor League Baseball (uppercase I)
    r'^MLS NEXT PRO \d+',       # MLS development league
    r'^MLS \d+ \|',             # MLS with pipe separator
    r'^NCAAF \d+ :',            # NCAAF with space before colon
    r'^WNBA \d+:?',             # WNBA channels (colon optional)
    r'^NFL \d+',                # NFL numbered event channels
    r'^NHL: [A-Z]',             # NHL team-specific channels
    r'^UFC \d+',                # UFC numbered event channels
    r'^LIVE EVENT \d+',         # Generic live event channels

    # US Networks and Streaming Platforms
    r'^US:',                     # US: prefix (no space)
    r'^US :',                    # US : prefix (with space)
    r'^\[Xumo\]',               # Xumo streaming platform
    r'^\[Stirr\]',              # Stirr streaming platform
    r'^\[Tubi\]',               # Tubi streaming platform

    # Cable/Satellite Providers
    r'^Spectrum',                # Spectrum channels
    r'^SPECTRUM',                # SPECTRUM (uppercase)

    # Regional/Local TV Stations
    r'^[A-Z]{2} \|',            # State code | City | Network format
]

def extract_channel_name(line):
    """Extract channel name from EXTINF line."""
    match = re.search(r'#EXTINF:-1,(.+)$', line)
    return match.group(1) if match else None

def extract_family_prefix(channel_name):
    """Extract the family prefix (everything before the number)."""
    # Remove number and everything after
    match = re.match(r'^([^\d]+)', channel_name)
    return match.group(1).strip() if match else channel_name

def matches_known_pattern(channel_name):
    """Check if channel matches any known pattern."""
    for pattern in KNOWN_FAMILIES:
        if re.match(pattern, channel_name):
            return True
    return False

def is_vod_url(url):
    """Check if URL is VOD (movie or series) content."""
    url_lower = url.lower()
    return '/movie/' in url_lower or '/series/' in url_lower

def is_live_event_channel(channel_name):
    """
    Check if channel is a Live Event channel (numbered stream).
    Live Event channels are specifically patterns that match known
    sports/event streaming services with numbered channels.
    """
    # Define patterns specific to Live Event channels (numbered streams)
    live_event_patterns = [
        r'^BIG10\+\s+\d+',
        r'^Bundesliga\s+\d+',
        r'^DAZN\s+[A-Z]{2}\s+\d+',
        r'^EPL\s+\d+',
        r'^ESPN\+\s+\d+',
        r'^Fanatiz\s+\d+',
        r'^Flo\s+(Football|Racing|Sports)\s+\d+',
        r'^La Liga\s+\d+',
        r'^Ligue1\s+\d+',
        r'^LIVE EVENT\s+\d+',
        r'^MAX\s+[A-Z]{2}\s+\d+',
        r'^MLB\s+\d+',
        r'^MLS\s+(NEXT PRO\s+)?\d+',
        r'^Mi?LB\s+\d+',  # Matches both MiLB and MILB
        r'^NBA\s+\d+',
        r'^NCAAF\s+\d+',
        r'^NFL\s+\d+',
        r'^NHL\s+(\||:)\s*\d+',
        r'^Paramount\+\s+\d+',
        r'^Peacock\s+\d+',
        r'^Prime US\s+\d+',
        r'^Serie A\s+\d+',
        r'^Sportsnet\+\s+\d+',
        r'^Tennis\s+\d+',
        r'^TSN\+\s+\d+',
        r'^UEFA\s+(Champions|Europa)\s+League\s+\d+',
        r'^UFC\s+\d+',
        r'^Viaplay\s+[A-Z]{2}\s+\d+',
        r'^WNBA\s+\d+',
    ]

    for pattern in live_event_patterns:
        if re.match(pattern, channel_name):
            return True
    return False

def main():
    if len(sys.argv) != 2:
        print("Usage: python verify_channels.py <path_to_m3u_file>")
        sys.exit(1)

    m3u_file = sys.argv[1]

    total_channels = 0
    vod_channels = 0
    live_tv_channels = 0
    matched_channels = 0
    unmatched_channels = []
    family_counts = defaultdict(int)

    # Track Live Event channels separately
    live_event_total = 0
    live_event_matched = 0
    live_event_unmatched = []
    regular_tv_total = 0
    regular_tv_matched = 0
    regular_tv_unmatched = []

    print("Analyzing M3U file...\n")

    with open(m3u_file, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith('#EXTINF:-1,'):
            total_channels += 1
            channel_name = extract_channel_name(line)

            # Get the URL from the next non-empty, non-comment line
            url = ""
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if next_line and not next_line.startswith('#'):
                    url = next_line
                    break
                j += 1

            # Skip VOD content (movies and series)
            if is_vod_url(url):
                vod_channels += 1
                i = j + 1
                continue

            # This is a live TV channel
            live_tv_channels += 1

            if channel_name:
                family = extract_family_prefix(channel_name)
                family_counts[family] += 1

                # Check if this is a Live Event channel
                is_live_event = is_live_event_channel(channel_name)
                is_matched = matches_known_pattern(channel_name)

                if is_matched:
                    matched_channels += 1
                else:
                    unmatched_channels.append(channel_name)

                # Categorize as Live Event or Regular TV
                if is_live_event:
                    live_event_total += 1
                    if is_matched:
                        live_event_matched += 1
                    else:
                        live_event_unmatched.append(channel_name)
                else:
                    regular_tv_total += 1
                    if is_matched:
                        regular_tv_matched += 1
                    else:
                        regular_tv_unmatched.append(channel_name)

            i = j + 1
        else:
            i += 1

    # Print results
    print(f"{'='*80}")
    print(f"CHANNEL VERIFICATION REPORT (Live TV Only)")
    print(f"{'='*80}\n")

    print(f"Total channels found: {total_channels}")
    print(f"  - VOD (movies/series): {vod_channels}")
    print(f"  - Live TV channels: {live_tv_channels}")
    print(f"")

    if live_tv_channels > 0:
        print(f"Overall Live TV Analysis:")
        print(f"  Matched by known patterns: {matched_channels} ({matched_channels/live_tv_channels*100:.1f}%)")
        print(f"  Unmatched channels: {len(unmatched_channels)} ({len(unmatched_channels)/live_tv_channels*100:.1f}%)")
        print(f"")

        print(f"Breakdown by Channel Type:")
        print(f"")
        print(f"  Live Event Channels (numbered streams):")
        print(f"    Total: {live_event_total}")
        if live_event_total > 0:
            print(f"    Matched: {live_event_matched} ({live_event_matched/live_event_total*100:.1f}%)")
            print(f"    Unmatched: {len(live_event_unmatched)} ({len(live_event_unmatched)/live_event_total*100:.1f}%)")
        print(f"")
        print(f"  Regular TV Channels (standard channels):")
        print(f"    Total: {regular_tv_total}")
        if regular_tv_total > 0:
            print(f"    Matched: {regular_tv_matched} ({regular_tv_matched/regular_tv_total*100:.1f}%)")
            print(f"    Unmatched: {len(regular_tv_unmatched)} ({len(regular_tv_unmatched)/regular_tv_total*100:.1f}%)")
    print()

    print(f"{'='*80}")
    print(f"CHANNEL FAMILIES (sorted by count)")
    print(f"{'='*80}\n")

    for family, count in sorted(family_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{family:40} {count:4} channels")

    if unmatched_channels:
        print(f"\n{'='*80}")
        print(f"UNMATCHED CHANNELS (first 50)")
        print(f"{'='*80}\n")

        for channel in unmatched_channels[:50]:
            print(f"  - {channel}")

        if len(unmatched_channels) > 50:
            print(f"\n  ... and {len(unmatched_channels) - 50} more")

    print(f"\n{'='*80}")

    if live_tv_channels == 0:
        print("⚠ No live TV channels found!")
    elif len(unmatched_channels) == 0:
        print("✓ All live TV channels matched! The list appears comprehensive.")
    elif len(unmatched_channels) / live_tv_channels < 0.05:
        print("✓ List looks good! Less than 5% unmatched (likely non-sports channels).")
    else:
        print("⚠ Review unmatched channels above - may need to add more families.")

    print(f"{'='*80}\n")

if __name__ == '__main__':
    main()
