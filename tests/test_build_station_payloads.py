"""Tests for archival and payload generation logic in build_station_payloads.py."""

import json
import pytest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, mock_open
import tempfile
import shutil

from bin.build_station_payloads import (
    _should_archive,
    _format_day_label,
    build_payloads,
)


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for output files."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_existing_payload():
    """Create a sample existing payload structure."""
    return {
        "meta": {
            "generatedAt": "2026-01-12T10:00:00+00:00",
            "climateDayStart": "2026-01-12T08:00:00+00:00",
            "climateDayEnd": "2026-01-13T08:00:00+00:00",
            "climateDayLabel": "2026-01-12",
        },
        "data": [
            {
                "stid": "SFOC1",
                "name": "San Francisco",
                "dateTime": "2026-01-12T20:00:00Z",
            }
        ],
    }


class TestFormatDayLabel:
    """Tests for _format_day_label function."""

    def test_format_day_label_utc(self):
        """Test formatting with UTC datetime."""
        day_start = datetime(2026, 1, 12, 8, 0, 0, tzinfo=timezone.utc)
        result = _format_day_label(day_start)
        # Should convert to Pacific time (00:00 PST = 08:00 UTC)
        assert result == "2026-01-12"

    def test_format_day_label_different_date(self):
        """Test formatting with different dates."""
        day_start1 = datetime(2026, 1, 12, 8, 0, 0, tzinfo=timezone.utc)
        day_start2 = datetime(2026, 1, 13, 8, 0, 0, tzinfo=timezone.utc)
        
        assert _format_day_label(day_start1) == "2026-01-12"
        assert _format_day_label(day_start2) == "2026-01-13"


class TestShouldArchive:
    """Tests for _should_archive function."""

    def test_no_existing_file(self, temp_output_dir):
        """Test when output file doesn't exist."""
        with patch('bin.build_station_payloads.OUTPUT_PATH', temp_output_dir / "station_payloads.json"):
            result = _should_archive("2026-01-13")
            assert result is False

    def test_same_day_no_archive(self, temp_output_dir, sample_existing_payload):
        """Test when current day matches existing file - no archive needed."""
        output_path = temp_output_dir / "station_payloads.json"
        output_path.write_text(json.dumps(sample_existing_payload), encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            result = _should_archive("2026-01-12")
            assert result is False

    def test_new_day_archive_needed(self, temp_output_dir, sample_existing_payload):
        """Test when new day detected - archive is needed."""
        output_path = temp_output_dir / "station_payloads.json"
        output_path.write_text(json.dumps(sample_existing_payload), encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            result = _should_archive("2026-01-13")
            assert result is True

    def test_corrupted_json_no_archive(self, temp_output_dir):
        """Test when existing file has corrupted JSON."""
        output_path = temp_output_dir / "station_payloads.json"
        output_path.write_text("{ invalid json }", encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            result = _should_archive("2026-01-13")
            assert result is False

    def test_missing_meta_no_archive(self, temp_output_dir):
        """Test when existing file missing meta data."""
        output_path = temp_output_dir / "station_payloads.json"
        output_path.write_text(json.dumps({"data": []}), encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            result = _should_archive("2026-01-13")
            assert result is False

    def test_missing_climate_day_label(self, temp_output_dir):
        """Test when existing file missing climateDayLabel."""
        output_path = temp_output_dir / "station_payloads.json"
        payload = {"meta": {"generatedAt": "2026-01-12T10:00:00Z"}, "data": []}
        output_path.write_text(json.dumps(payload), encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            result = _should_archive("2026-01-13")
            assert result is False


class TestBuildPayloads:
    """Tests for build_payloads function."""

    @patch('bin.build_station_payloads.fetch_synoptic_data')
    @patch('bin.build_station_payloads.build_station_payload')
    def test_build_payloads_structure(self, mock_build_station, mock_fetch):
        """Test that build_payloads returns correct structure."""
        # Mock the fetch response
        mock_fetch.return_value = ([], [], [])
        
        # Mock build_station_payload to return empty lists
        mock_build_station.return_value = []
        
        now = datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc)
        today_payload = build_payloads(now)
        
        # Verify structure
        assert "meta" in today_payload
        assert "data" in today_payload
        
        # Verify meta fields
        assert "generatedAt" in today_payload["meta"]
        assert "climateDayStart" in today_payload["meta"]
        assert "climateDayEnd" in today_payload["meta"]
        assert "climateDayLabel" in today_payload["meta"]

    @patch('bin.build_station_payloads.fetch_synoptic_data')
    @patch('bin.build_station_payloads.build_station_payload')
    def test_build_payloads_time_windows(self, mock_build_station, mock_fetch):
        """Test that correct time windows are used for today."""
        mock_fetch.return_value = ([], [], [])
        mock_build_station.return_value = []
        
        now = datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc)
        today_payload = build_payloads(now)
        
        # Today should be 2026-01-13 08:00 - 2026-01-14 08:00
        assert today_payload["meta"]["climateDayStart"] == "2026-01-13T08:00:00+00:00"
        assert today_payload["meta"]["climateDayEnd"] == "2026-01-14T08:00:00+00:00"

    @patch('bin.build_station_payloads.fetch_synoptic_data')
    @patch('bin.build_station_payloads.build_station_payload')
    def test_build_payloads_combines_asos_and_hads(self, mock_build_station, mock_fetch):
        """Test that ASOS and HADS data are combined."""
        mock_fetch.return_value = ([], [], [])
        
        # Mock different return values for ASOS and HADS
        mock_build_station.side_effect = [
            [{"stid": "ASOS1"}],  # ASOS today
            [{"stid": "HADS1"}],  # HADS today
        ]
        
        now = datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc)
        today_payload = build_payloads(now)
        
        # Verify data is combined
        assert len(today_payload["data"]) == 2
        assert today_payload["data"][0]["stid"] == "ASOS1"
        assert today_payload["data"][1]["stid"] == "HADS1"


class TestOSOCacheYesterday:
    """Tests for OSO cache yesterday integration."""

    @patch('bin.build_station_payloads.fetch_synoptic_data')
    @patch('bin.build_station_payloads.build_station_payload')
    @patch('bin.build_station_payloads.USE_DEV_PATHS', True)
    def test_yesterday_payload_uses_oso_cache_yesterday(
        self, mock_build_station, mock_fetch, temp_output_dir
    ):
        """Test that yesterday's payload generation uses oso_cache_yesterday.json for OSO stations."""
        # Setup mock data
        mock_fetch.return_value = (
            [{"STID": "KCCR"}],  # stationsA (ASOS)
            [{"STID": "KCCR"}],  # stationsB (ASOS metadata)
            [{"STID": "SFOC1"}],  # stationsC (HADS/OSO)
        )
        
        # Mock return values for build_station_payload
        mock_build_station.return_value = [{"stid": "TEST"}]
        
        # Setup paths
        output_path = temp_output_dir / "station_payloads.json"
        yesterday_output_path = temp_output_dir / "station_payloads_yesterday.json"
        
        # Create existing payload from previous day to trigger archival
        existing_payload = {
            "meta": {
                "generatedAt": "2026-02-16T10:00:00+00:00",
                "climateDayStart": "2026-02-16T08:00:00+00:00",
                "climateDayEnd": "2026-02-17T08:00:00+00:00",
                "climateDayLabel": "2026-02-16",
            },
            "data": [],
        }
        output_path.write_text(json.dumps(existing_payload), encoding='utf-8')
        
        # Patch paths
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path), \
             patch('bin.build_station_payloads.YESTERDAY_OUTPUT_PATH', yesterday_output_path), \
             patch('bin.build_station_payloads.ROOT_DIR', temp_output_dir):
            
            # Import and run main
            from bin.build_station_payloads import main
            
            # Mock the argument parser to avoid command line args
            with patch('sys.argv', ['build_station_payloads.py', '--as-of', '2026-02-17T10:00:00Z']):
                main()
        
        # Verify build_station_payload was called for yesterday's HADS with oso_cache_yesterday
        # Find the call that was for HADS type yesterday
        hads_calls = [
            call for call in mock_build_station.call_args_list
            if call[1].get('type') == 'HADS' and call[1].get('is_current_day') is False
        ]
        
        assert len(hads_calls) == 1, "Should have exactly one HADS call for yesterday"
        
        # Verify the oso_cache_file parameter was passed with yesterday's cache
        yesterday_call = hads_calls[0]
        oso_cache_arg = yesterday_call[1].get('oso_cache_file')
        
        assert oso_cache_arg is not None, "oso_cache_file should be passed for yesterday's HADS"
        assert 'oso_cache_yesterday.json' in oso_cache_arg, \
            f"Expected oso_cache_yesterday.json in path, got: {oso_cache_arg}"


class TestArchivalWorkflow:
    """Integration tests for the archival workflow."""

    def test_first_run_no_archive(self, temp_output_dir):
        """Test first run when no existing file exists."""
        output_path = temp_output_dir / "station_payloads.json"
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            should_archive = _should_archive("2026-01-13")
            assert should_archive is False

    def test_same_day_multiple_runs(self, temp_output_dir, sample_existing_payload):
        """Test multiple runs on the same climate day."""
        output_path = temp_output_dir / "station_payloads.json"
        output_path.write_text(json.dumps(sample_existing_payload), encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            # First check - same day
            should_archive = _should_archive("2026-01-12")
            assert should_archive is False
            
            # Second check - still same day
            should_archive = _should_archive("2026-01-12")
            assert should_archive is False

    def test_day_transition(self, temp_output_dir, sample_existing_payload):
        """Test detection of day transition."""
        output_path = temp_output_dir / "station_payloads.json"
        yesterday_path = temp_output_dir / "station_payloads_yesterday.json"
        
        # Write yesterday's payload
        output_path.write_text(json.dumps(sample_existing_payload), encoding='utf-8')
        
        with patch('bin.build_station_payloads.OUTPUT_PATH', output_path):
            # Check for new day
            should_archive = _should_archive("2026-01-13")
            assert should_archive is True
            
            # Simulate the archive
            if should_archive:
                shutil.copy2(output_path, yesterday_path)
                
                # Create new payload for today
                new_payload = sample_existing_payload.copy()
                new_payload["meta"] = sample_existing_payload["meta"].copy()
                new_payload["meta"]["climateDayLabel"] = "2026-01-13"
                output_path.write_text(json.dumps(new_payload), encoding='utf-8')
            
            # Verify files exist
            assert output_path.exists()
            assert yesterday_path.exists()
            
            # Verify yesterday file has old date
            yesterday_data = json.loads(yesterday_path.read_text(encoding='utf-8'))
            assert yesterday_data["meta"]["climateDayLabel"] == "2026-01-12"
            
            # Next check should not trigger archive
            should_archive_again = _should_archive("2026-01-13")
            assert should_archive_again is False
