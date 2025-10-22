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

                if matches_known_pattern(channel_name):
                    matched_channels += 1
                else:
                    unmatched_channels.append(channel_name)

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
        print(f"Live TV Analysis:")
        print(f"  Matched by known patterns: {matched_channels} ({matched_channels/live_tv_channels*100:.1f}%)")
        print(f"  Unmatched channels: {len(unmatched_channels)} ({len(unmatched_channels)/live_tv_channels*100:.1f}%)")
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
