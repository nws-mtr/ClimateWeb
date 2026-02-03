"""Tests for build_station_payloads.py functions."""
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

import bin.build_station_payloads as builder


def test_format_day_label_returns_pacific_date():
    day_start = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    result = builder._format_day_label(day_start)
    # 2024-01-02 08:00 UTC is 2024-01-02 00:00 Pacific
    assert result == "2024-01-02"


def test_parse_as_of_handles_none_and_empty():
    assert builder._parse_as_of(None) is None
    assert builder._parse_as_of("") is None
    assert builder._parse_as_of("   ") is None


def test_parse_as_of_parses_iso_with_z():
    result = builder._parse_as_of("2024-01-02T08:00:00Z")
    assert result == datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)


def test_parse_as_of_parses_iso_with_offset():
    result = builder._parse_as_of("2024-01-02T00:00:00-08:00")
    # Converts to UTC
    assert result == datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)


def test_parse_as_of_adds_utc_if_naive():
    result = builder._parse_as_of("2024-01-02T08:00:00")
    assert result.tzinfo == timezone.utc


def test_parse_as_of_raises_on_invalid():
    with pytest.raises(SystemExit) as excinfo:
        builder._parse_as_of("not-a-date")
    assert "Invalid --as-of value" in str(excinfo.value)


def test_should_generate_yesterday_returns_true_if_no_marker(tmp_path, monkeypatch):
    marker = tmp_path / ".yesterday_marker"
    monkeypatch.setattr(builder, "YESTERDAY_MARKER_PATH", marker)
    
    now = datetime(2024, 1, 3, 8, 0, tzinfo=timezone.utc)
    assert builder.should_generate_yesterday(now) is True


def test_should_generate_yesterday_returns_true_on_new_day(tmp_path, monkeypatch):
    marker = tmp_path / ".yesterday_marker"
    monkeypatch.setattr(builder, "YESTERDAY_MARKER_PATH", marker)
    
    # Write marker from yesterday
    last_run = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    marker.write_text(last_run.isoformat())
    
    # Now is next climate day
    now = datetime(2024, 1, 3, 9, 0, tzinfo=timezone.utc)
    assert builder.should_generate_yesterday(now) is True


def test_should_generate_yesterday_returns_false_same_day(tmp_path, monkeypatch):
    marker = tmp_path / ".yesterday_marker"
    monkeypatch.setattr(builder, "YESTERDAY_MARKER_PATH", marker)
    
    # Write marker from earlier today
    last_run = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    marker.write_text(last_run.isoformat())
    
    # Now is same climate day
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    assert builder.should_generate_yesterday(now) is False


def test_should_generate_yesterday_returns_true_on_corrupt_marker(tmp_path, monkeypatch):
    marker = tmp_path / ".yesterday_marker"
    monkeypatch.setattr(builder, "YESTERDAY_MARKER_PATH", marker)
    
    marker.write_text("corrupt data")
    
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    assert builder.should_generate_yesterday(now) is True


def test_build_today_payload_structure(monkeypatch):
    """Test that build_today_payload returns proper structure."""
    # Mock fetch_synoptic_data
    def fake_fetch(current_time=None):
        return [], [], []
    
    # Mock build_station_payload
    def fake_build(stations_a, stations_b=None, *, type, day_start, day_end, now, is_current_day):
        return []
    
    monkeypatch.setattr(builder, "fetch_synoptic_data", fake_fetch)
    monkeypatch.setattr(builder, "build_station_payload", fake_build)
    
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    payload = builder.build_today_payload(now)
    
    assert "meta" in payload
    assert "data" in payload
    assert "generatedAt" in payload["meta"]
    assert "climateDayStart" in payload["meta"]
    assert "climateDayEnd" in payload["meta"]
    assert "climateDayLabel" in payload["meta"]
    assert isinstance(payload["data"], list)


def test_build_payloads_creates_both_payloads(monkeypatch):
    """Test that build_payloads returns both today and yesterday."""
    # Mock fetch_synoptic_data
    def fake_fetch(current_time=None):
        return [], [], []
    
    # Mock build_station_payload to return different data based on is_current_day
    def fake_build(stations_a, stations_b=None, *, type, day_start, day_end, now, is_current_day):
        if is_current_day:
            return [{"stid": "TODAY"}]
        else:
            return [{"stid": "YESTERDAY"}]
    
    monkeypatch.setattr(builder, "fetch_synoptic_data", fake_fetch)
    monkeypatch.setattr(builder, "build_station_payload", fake_build)
    
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    today_payload, yesterday_payload = builder.build_payloads(now)
    
    # Check structure
    assert "meta" in today_payload
    assert "data" in today_payload
    assert "meta" in yesterday_payload
    assert "data" in yesterday_payload
    
    # Verify different climate day windows
    assert today_payload["meta"]["climateDayStart"] != yesterday_payload["meta"]["climateDayStart"]


def test_build_yesterday_payload_structure(monkeypatch):
    """Test that build_yesterday_payload has correct structure."""
    # Mock build_station_payload
    def fake_build(stations_a, stations_b=None, *, type, day_start, day_end, now, is_current_day):
        return [{"stid": f"{type}_STATION"}]
    
    monkeypatch.setattr(builder, "build_station_payload", fake_build)
    
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    stationsA, stationsB, stationsC = [], [], []
    
    payload = builder.build_yesterday_payload(stationsA, stationsB, stationsC, now)
    
    assert "meta" in payload
    assert "data" in payload
    assert "generatedAt" in payload["meta"]
    assert "climateDayStart" in payload["meta"]
    assert "climateDayEnd" in payload["meta"]
    assert "climateDayLabel" in payload["meta"]
    assert len(payload["data"]) == 2  # ASOS + HADS


def test_format_day_label_handles_timezone_conversion():
    """Test that _format_day_label converts to Pacific time correctly."""
    # 8 AM UTC on Jan 2 = midnight Pacific on Jan 2
    day_start = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    result = builder._format_day_label(day_start)
    assert result == "2024-01-02"
    
    # 7 AM UTC on Jan 2 = 11 PM Pacific on Jan 1
    day_start = datetime(2024, 1, 2, 7, 0, tzinfo=timezone.utc)
    result = builder._format_day_label(day_start)
    assert result == "2024-01-01"


def test_parse_as_of_returns_none_for_whitespace():
    """Test that _parse_as_of handles whitespace-only input."""
    assert builder._parse_as_of("   \t  \n  ") is None


def test_main_writes_both_payloads_when_needed(monkeypatch, tmp_path, capsys):
    """Test main function with --force-yesterday flag."""
    # Mock paths
    output_path = tmp_path / "station_payloads.json"
    yesterday_path = tmp_path / "station_payloads_yesterday.json"
    marker_path = tmp_path / ".yesterday_marker"
    
    monkeypatch.setattr(builder, "OUTPUT_PATH", output_path)
    monkeypatch.setattr(builder, "YESTERDAY_OUTPUT_PATH", yesterday_path)
    monkeypatch.setattr(builder, "YESTERDAY_MARKER_PATH", marker_path)
    
    # Mock build_payloads
    def fake_build_payloads(now):
        return {"meta": {}, "data": []}, {"meta": {}, "data": []}
    
    monkeypatch.setattr(builder, "build_payloads", fake_build_payloads)
    
    # Mock sys.argv
    monkeypatch.setattr("sys.argv", ["build_station_payloads.py", "--force-yesterday"])
    
    builder.main()
    
    # Check files were created
    assert output_path.exists()
    assert yesterday_path.exists()
    assert marker_path.exists()
    
    # Check output
    captured = capsys.readouterr()
    assert "Generated yesterday's payload" in captured.out


def test_main_skips_yesterday_when_not_needed(monkeypatch, tmp_path, capsys):
    """Test main function skips yesterday when marker is recent."""
    # Mock paths
    output_path = tmp_path / "station_payloads.json"
    yesterday_path = tmp_path / "station_payloads_yesterday.json"
    marker_path = tmp_path / ".yesterday_marker"
    
    monkeypatch.setattr(builder, "OUTPUT_PATH", output_path)
    monkeypatch.setattr(builder, "YESTERDAY_OUTPUT_PATH", yesterday_path)
    monkeypatch.setattr(builder, "YESTERDAY_MARKER_PATH", marker_path)
    
    # Create recent marker
    marker_path.write_text(datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc).isoformat())
    
    # Mock build_today_payload
    def fake_build_today(now):
        return {"meta": {}, "data": []}
    
    monkeypatch.setattr(builder, "build_today_payload", fake_build_today)
    
    # Mock sys.argv with --as-of in same climate day
    monkeypatch.setattr("sys.argv", ["build_station_payloads.py", "--as-of", "2024-01-02T12:00:00Z"])
    
    builder.main()
    
    # Check only today's file was created
    assert output_path.exists()
    assert not yesterday_path.exists()
    
    # Check output
    captured = capsys.readouterr()
    assert "Skipped yesterday's payload" in captured.out
