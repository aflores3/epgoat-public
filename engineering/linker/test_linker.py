#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Unit tests for linker_prototype.py

Tests pattern matching, colon handling, and channel classification.
"""

import pytest
from linker_prototype import (
    match_prefix_and_shell,
    classify_channel,
    try_parse_time,
)
import datetime as dt
from zoneinfo import ZoneInfo


class TestPatternMatching:
    """Test channel pattern matching with various formats."""

    def test_ncaaf_channels(self):
        """Test NCAAF pattern with various spacing formats."""
        test_cases = [
            ("NCAAF 01 : Middle Tennessee at Delaware @ 07:30 PM ET", True, "NCAAF"),
            ("NCAAF 01: Game", True, "NCAAF"),
            ("NCAAF 01 :", True, "NCAAF"),
            ("NCAAF 100: Championship", True, "NCAAF"),
            ("NCAAF Game", False, None),  # No number
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family, f"Wrong family for: {channel_name}"

    def test_mls_channels(self):
        """Test MLS pattern variations."""
        test_cases = [
            ("MLS 01 : Game", True, "MLS"),
            ("MLS 05:", True, "MLS"),
            ("MLS 10", False, None),  # Missing colon
            ("MLS NEXT PRO 01", True, "MLS NEXT PRO"),
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family

    def test_gaago_dirtvision(self):
        """Test patterns with colons in the middle."""
        test_cases = [
            ("GAAGO : GAME 01", True, "GAAGO"),
            ("GAAGO:GAME 01", True, "GAAGO"),
            ("GAAGO : GAME 10", True, "GAAGO"),
            ("Dirtvision : EVENT 01", True, "Dirtvision"),
            ("Dirtvision:EVENT 01", True, "Dirtvision"),
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family

    def test_nfl_variations(self):
        """Test NFL channel variations."""
        test_cases = [
            ("NFL 01: Game", True, "NFL"),
            ("NFL 01 :", True, "NFL"),
            ("NFL Game Pass 1", True, "NFL Game Pass"),
            ("NFL Game Pass 10:", True, "NFL Game Pass"),
            ("NFL Multi Screen / HDR 1", True, "NFL Multi Screen"),
            ("NFL | 01 -", True, "NFL |"),
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family

    def test_paramount_espn_plus(self):
        """Test streaming services with optional colons."""
        test_cases = [
            ("Paramount+ 01:", True, "Paramount+"),
            ("Paramount+ 01 :", True, "Paramount+"),
            ("Paramount+ 100:", True, "Paramount+"),
            ("ESPN+ 01:", True, "ESPN+"),
            ("ESPN+ 02", True, "ESPN+"),  # Colon optional
            ("ESPN+ 100", True, "ESPN+"),
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family

    def test_flexible_digits(self):
        """Test that patterns accept flexible digit counts."""
        test_cases = [
            ("NCAAF 1:", True, "NCAAF"),    # Single digit
            ("NCAAF 01:", True, "NCAAF"),   # Two digits
            ("NCAAF 100:", True, "NCAAF"),  # Three digits
            ("NCAAF 1000:", True, "NCAAF"), # Four digits
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family


class TestChannelClassification:
    """Test channel classification (generic vs event)."""

    def test_generic_channels(self):
        """Test channels classified as generic."""
        # These should be generic (no meaningful payload)
        test_cases = [
            "NCAAF 03 :",
            "NBA 05:",
            "MLB 10: ",
        ]
        for channel_name in test_cases:
            matched, family, match_obj = match_prefix_and_shell(channel_name)
            assert matched, f"Pattern should match: {channel_name}"
            classif = classify_channel(channel_name, family, match_obj)
            assert classif.classification == "generic", f"Should be generic: {channel_name}"

    def test_event_channels(self):
        """Test channels classified as events."""
        test_cases = [
            "NCAAF 01 : Middle Tennessee at Delaware @ 07:30 PM ET",
            "NBA 02: Lakers vs Celtics @ 8pm ET",
            "Paramount+ 05: UEFA Champions League",
        ]
        for channel_name in test_cases:
            matched, family, match_obj = match_prefix_and_shell(channel_name)
            assert matched, f"Pattern should match: {channel_name}"
            classif = classify_channel(channel_name, family, match_obj)
            assert classif.classification == "event", f"Should be event: {channel_name}"


class TestTimeParsing:
    """Test time parsing from channel names."""

    def setup_method(self):
        """Setup timezone for tests."""
        self.central = ZoneInfo("America/Chicago")
        self.today = dt.date(2025, 10, 22)

    def test_parse_time_with_am_pm(self):
        """Test parsing 12-hour time format."""
        payload = "Middle Tennessee at Delaware @ 07:30 PM ET"
        result = try_parse_time(payload, 2025, self.central, self.today)
        assert result is not None, "Should parse time"
        assert result.hour == 18, "Should be 6 PM CT (7 PM ET - 1 hour)"
        assert result.minute == 30

    def test_parse_time_with_date(self):
        """Test parsing time with date."""
        payload = "Game @ Oct 22 03:00 PM ET"
        result = try_parse_time(payload, 2025, self.central, self.today)
        assert result is not None, "Should parse time with date"
        assert result.month == 10
        assert result.day == 22

    def test_parse_time_without_date(self):
        """Test parsing time without date (uses context date)."""
        payload = "Game @ 08:00 PM CT"
        result = try_parse_time(payload, 2025, self.central, self.today)
        assert result is not None, "Should parse time"
        assert result.date() == self.today
        assert result.hour == 20

    def test_parse_tba(self):
        """Test that 'Time TBA' returns None."""
        payload = "Game (Time TBA)"
        result = try_parse_time(payload, 2025, self.central, self.today)
        assert result is None, "TBA should return None"


class TestEdgeCases:
    """Test edge cases and special scenarios."""

    def test_empty_channel_name(self):
        """Test handling of empty channel name."""
        matched, family, _ = match_prefix_and_shell("")
        assert not matched, "Empty string should not match"

    def test_none_channel_name(self):
        """Test handling of None channel name."""
        matched, family, _ = match_prefix_and_shell(None)
        assert not matched, "None should not match"

    def test_whitespace_only(self):
        """Test handling of whitespace-only channel name."""
        matched, family, _ = match_prefix_and_shell("   ")
        assert not matched, "Whitespace only should not match"

    def test_special_characters(self):
        """Test channels with special characters."""
        test_cases = [
            ("MLS Espanolⓧ 01", True, "MLS Espanol"),
            ("SEC+ / ACC extra 01", True, "SEC+/ACC extra"),
        ]
        for channel_name, should_match, expected_family in test_cases:
            matched, family, _ = match_prefix_and_shell(channel_name)
            assert matched == should_match, f"Failed for: {channel_name}"
            if should_match:
                assert family == expected_family


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
