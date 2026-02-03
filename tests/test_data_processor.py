from datetime import datetime, timezone

import numpy as np
import pytest

import src.data_processor as dp


@pytest.fixture(autouse=True)
def restore_midnight(monkeypatch):
    """Ensure get_midnight can be patched per-test without leaking state."""
    original = dp.get_midnight
    yield
    monkeypatch.setattr(dp, "get_midnight", original)


def test_mm_to_in_and_c_to_f_handle_none_and_values():
    assert dp.mm_to_in(None) is None
    assert dp.mm_to_in(25.4) == pytest.approx(1.0)
    assert dp.c_to_f(None) is None
    assert dp.c_to_f(0) == 32
    assert dp.c_to_f(10) == 50


def test_compute_daily_from_cumulative_computes_since_midnight(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    cumulative_obs = [10.0, 12.5]
    timestamps = ["2024-01-01T23:00:00Z", "2024-01-02T06:00:00Z"]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    daily = dp._compute_daily_from_cumulative(cumulative_obs, timestamps, day_start, day_end)
    assert daily == pytest.approx(2.5)


def test_compute_daily_from_cumulative_returns_none_on_reset(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    cumulative_obs = [5.0, 1.0]
    timestamps = ["2024-01-01T23:00:00Z", "2024-01-02T02:00:00Z"]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    assert dp._compute_daily_from_cumulative(cumulative_obs, timestamps, day_start, day_end) is None


def test_compute_precip_from_hourly_daily_filters_and_sums(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    hourly = [0.1, 0.3, 0.5, 1.0]
    timestamps = [
        "2024-01-01T22:00:00Z",  # before midnight -> ignored
        "2024-01-02T00:30:00Z",  # below threshold -> 0
        "2024-01-02T02:00:00Z",  # counted
        "2024-01-02T04:00:00Z",  # counted
    ]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    total = dp._compute_precip_from_hourly(hourly, timestamps, day_start=day_start, day_end=day_end, period="daily")
    assert total == pytest.approx(1.8)


def test_compute_precip_from_hourly_wateryear_includes_all(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    hourly = [0.1, 0.3, 0.5, 1.0]
    timestamps = [
        "2024-01-01T22:00:00Z",
        "2024-01-02T00:30:00Z",
        "2024-01-02T02:00:00Z",
        "2024-01-02T04:00:00Z",
    ]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    total = dp._compute_precip_from_hourly(hourly, timestamps, day_start=day_start, day_end=day_end, period="wateryear")
    assert total == pytest.approx(1.8)


def test_compute_precip_from_hourly_validates_period():
    day_start = datetime(2024, 1, 2, tzinfo=timezone.utc)
    day_end = day_start + __import__('datetime').timedelta(days=1)
    with pytest.raises(ValueError):
        dp._compute_precip_from_hourly([], [], day_start=day_start, day_end=day_end, period="invalid")


def test_unwrap_cumulative_handles_resets_and_missing():
    values = [None, 1.0, 2.0, 1.5, 3.0]
    result = dp.unwrap_cumulative(values)
    assert result == [None, 0.0, 1.0, 1.0, 2.5]


def test_compute_daily_temp_range_uses_hourly_and_6hr(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    times = [
        "2024-01-01T22:00:00Z",
        "2024-01-02T01:00:00Z",
        "2024-01-02T05:00:00Z",
    ]
    air_temp = [5.0, 10.0, 8.0]
    max6 = [12.0, 15.0, 20.0]
    min6 = [2.0, 4.0, 6.0]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    daily_max, daily_min = dp._compute_daily_temp_range(air_temp, times, max6, min6, day_start=day_start, day_end=day_end)
    assert daily_max == pytest.approx(20.0)
    assert daily_min == pytest.approx(4.0)


def test_format_hads_builds_payload(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    # Use realistic ACIS values that are close to the station observations
    # wy_in from ACIS should be similar to wy_in_station (cumulative precip)
    # Station has 7mm cumulative = 0.28", so let's say ACIS has 0.25" (without today's 7mm)
    # norm_in = 0.2" for easy calculation (makes 0.56" = 280% of normal)
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [0.25, 0.2])
    monkeypatch.setattr(dp, "_parse_oso_file", lambda stid, now, home_dir, is_current_day=True: (None, None))

    station = {
        "STID": "H1",
        "NAME": "HADS Station",
        "ELEVATION": 10,
        "LATITUDE": 45.0,
        "LONGITUDE": -120.0,
        "OBSERVATIONS": {
            "air_temp_set_1": [10.0, 12.0, 14.0],
            "precip_accum_set_1": [0.0, 5.0, 7.0],
            "date_time": [
                "2024-01-01T23:00:00Z",
                "2024-01-02T02:00:00Z",
                "2024-01-02T04:00:00Z",
            ],
        },
    }

    now = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    day_start, day_end = dp.climate_day_window(now, days_ago=0)
    payload = dp.format_hads(station, day_start=day_start, day_end=day_end, now=now, is_current_day=True)

    assert payload["stid"] == "H1"
    assert payload["name"] == "HADS Station"
    assert payload["airTempF"] == 57  # 14C -> 57F
    assert payload["dailyMaxF"] == 57  # 14C -> 57F
    assert payload["dailyMinF"] == 54  # 12C -> 54F
    # daily_in would be calculated from the cumulative data for the day window
    # With is_current_day=True, waterYearIN = wy_in_station + daily_in
    # Since this is testing current day, the water year should include daily accumulation
    daily_in_mm = 7.0  # From precip_accum_set_1
    wy_in_station_mm = 7.0  # Latest from unwrapped cumulative
    daily_in = np.round(daily_in_mm / 25.4, 2)
    wy_in_station = np.round(wy_in_station_mm / 25.4, 2)
    assert payload["dailyAccumIN"] == pytest.approx(daily_in)
    # For current day: wy_in = wy_in_station + daily_in
    expected_wy = wy_in_station + daily_in
    assert payload["waterYearIN"] == pytest.approx(expected_wy)
    assert payload["waterYearNormIN"] == 0.2
    # pct = int((0.56 / 0.2) * 100) = int(280) = 280
    assert payload["percentOfNorm"] == 280


def test_format_asos_combines_station_data(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [50.0, 25.0])

    station_a = {
        "STID": "A1",
        "NAME": "ASOS Station",
        "ELEVATION": 20,
        "LATITUDE": 40.0,
        "LONGITUDE": -110.0,
        "OBSERVATIONS": {
            "air_temp_set_1": [5.0, 10.0, 8.0],
            "air_temp_high_6_hour_set_1": [12.0, 14.0, 16.0],
            "air_temp_low_6_hour_set_1": [1.0, 3.0, 5.0],
            "date_time": [
                "2024-01-01T22:00:00Z",
                "2024-01-02T02:00:00Z",
                "2024-01-02T04:00:00Z",
            ],
        },
    }

    station_b = {
        "STID": "B1",
        "OBSERVATIONS": {
            "precipitation": [
                {"total": 0.1, "last_report": "2024-01-01T23:00:00Z"},
                {"total": 0.3, "last_report": "2024-01-02T01:00:00Z"},
                {"total": 1.0, "last_report": "2024-01-02T03:00:00Z"},
            ]
        },
    }

    now = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    day_start, day_end = dp.climate_day_window(now, days_ago=0)
    payload = dp.format_asos(station_a, station_b, day_start=day_start, day_end=day_end, now=now, is_current_day=True)

    assert payload["stid"] == "A1"
    assert payload["name"] == "ASOS Station"
    assert payload["airTempF"] == dp.c_to_f(8.0)
    assert payload["dailyMaxF"] == dp.c_to_f(16.0)
    assert payload["dailyMinF"] == dp.c_to_f(3.0)
    daily_in = np.round((0.3 + 1.0) / 25.4, 2)
    assert payload["dailyAccumIN"] == pytest.approx(daily_in)
    # For current day (is_current_day=True): wy_in = 50.0 + daily_in
    assert payload["waterYearIN"] == pytest.approx(50.0 + daily_in)
    assert payload["waterYearNormIN"] == 25.0
    assert payload["percentOfNorm"] == 200


def test_build_station_payload_validates_input(monkeypatch):
    now = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    day_start, day_end = dp.climate_day_window(now, days_ago=0)
    
    with pytest.raises(ValueError):
        dp.build_station_payload([], type=None, day_start=day_start, day_end=day_end, now=now)

    with pytest.raises(ValueError):
        dp.build_station_payload([], type="UNKNOWN", day_start=day_start, day_end=day_end, now=now)

    with pytest.raises(ValueError):
        dp.build_station_payload([{}], [{}, {}], type="ASOS", day_start=day_start, day_end=day_end, now=now)

    monkeypatch.setattr(dp, "format_hads", lambda station, day_start, day_end, now, is_current_day: {"stid": station["STID"]})
    result = dp.build_station_payload([{"STID": "H1"}], type="HADS", day_start=day_start, day_end=day_end, now=now)
    assert result == [{"stid": "H1"}]

    monkeypatch.setattr(dp, "format_asos", lambda a, b, day_start, day_end, now, is_current_day: {"stid": a["STID"] + b["STID"]})
    result = dp.build_station_payload([{"STID": "A"}], [{"STID": "B"}], type="ASOS", day_start=day_start, day_end=day_end, now=now)
    assert result == [{"stid": "AB"}]


def test_climate_day_window_returns_8am_start():
    """Test that climate day starts at 8 AM UTC."""
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    start, end = dp.climate_day_window(now, days_ago=0)
    
    assert start == datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2024, 1, 3, 8, 0, tzinfo=timezone.utc)


def test_climate_day_window_yesterday():
    """Test that days_ago parameter works correctly."""
    now = datetime(2024, 1, 3, 12, 0, tzinfo=timezone.utc)
    start, end = dp.climate_day_window(now, days_ago=1)
    
    assert start == datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    assert end == datetime(2024, 1, 3, 8, 0, tzinfo=timezone.utc)


def test_get_midnight_returns_8am_utc():
    """Test that get_midnight returns 8 AM UTC (climate day start)."""
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    midnight = dp.get_midnight(now)
    
    assert midnight.hour == 8
    assert midnight.minute == 0
    assert midnight.second == 0


def test_get_midnight_before_8am_returns_previous_day():
    """Test that times before 8 AM get previous day's climate day."""
    now = datetime(2024, 1, 2, 6, 0, tzinfo=timezone.utc)
    midnight = dp.get_midnight(now)
    
    assert midnight == datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_load_oso_cache_returns_empty_on_missing_file(tmp_path):
    """Test that loading non-existent cache returns empty dict."""
    cache_file = str(tmp_path / "nonexistent.json")
    result = dp._load_oso_cache(cache_file)
    assert result == {}


def test_load_oso_cache_returns_empty_on_corrupt_file(tmp_path):
    """Test that loading corrupt cache returns empty dict."""
    cache_file = tmp_path / "cache.json"
    cache_file.write_text("not valid json{")
    result = dp._load_oso_cache(str(cache_file))
    assert result == {}


def test_load_oso_cache_loads_valid_file(tmp_path):
    """Test that loading valid cache works."""
    import json
    cache_file = tmp_path / "cache.json"
    data = {"SFOC1": {"day": "2024-01-02", "max_hi": 20.0, "min_lo": 10.0}}
    cache_file.write_text(json.dumps(data))
    
    result = dp._load_oso_cache(str(cache_file))
    assert result == data


def test_save_oso_cache_writes_json(tmp_path):
    """Test that saving cache writes valid JSON."""
    import json
    cache_file = tmp_path / "cache.json"
    data = {"SFOC1": {"day": "2024-01-02", "max_hi": 20.0, "min_lo": 10.0}}
    
    dp._save_oso_cache(str(cache_file), data)
    
    assert cache_file.exists()
    loaded = json.loads(cache_file.read_text())
    assert loaded == data


def test_parse_oso_file_returns_none_for_unknown_station(tmp_path):
    """Test that unknown station IDs return None."""
    result = dp._parse_oso_file("UNKNOWN", datetime.now(timezone.utc), str(tmp_path), is_current_day=True)
    assert result == (None, None)


def test_parse_oso_file_returns_none_for_missing_file(tmp_path):
    """Test that missing OSO file returns None."""
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    assert result == (None, None)


def test_parse_oso_file_returns_yesterday_cache(tmp_path):
    """Test that is_current_day=False reads from yesterday cache."""
    import json
    
    # Create yesterday cache
    yesterday_cache = tmp_path / "oso_cache_yesterday.json"
    cache_data = {
        "SFOC1": {
            "day": "2024-01-01",
            "max_hi": 22.0,
            "min_lo": 8.0,
            "last_update": "2024-01-01T20:00:00+00:00"
        }
    }
    yesterday_cache.write_text(json.dumps(cache_data))
    
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=False)
    
    assert result == (22.0, 8.0)


def test_parse_oso_file_returns_none_if_yesterday_cache_missing(tmp_path):
    """Test that is_current_day=False returns None if cache doesn't exist."""
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=False)
    
    assert result == (None, None)


def test_format_hads_with_yesterday_flag(monkeypatch):
    """Test that format_hads correctly uses is_current_day flag for water year calc."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [5.0, 10.0])
    monkeypatch.setattr(dp, "_parse_oso_file", lambda stid, now, home_dir, is_current_day: (None, None))

    station = {
        "STID": "H1",
        "NAME": "HADS Station",
        "ELEVATION": 10,
        "LATITUDE": 45.0,
        "LONGITUDE": -120.0,
        "OBSERVATIONS": {
            "air_temp_set_1": [10.0, 12.0, 14.0],
            "precip_accum_set_1": [0.0, 5.0, 7.0],
            "date_time": [
                "2024-01-01T23:00:00Z",
                "2024-01-02T02:00:00Z",
                "2024-01-02T04:00:00Z",
            ],
        },
    }

    now = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    day_start, day_end = dp.climate_day_window(now, days_ago=1)
    
    # Test with is_current_day=False (yesterday)
    payload = dp.format_hads(station, day_start=day_start, day_end=day_end, now=now, is_current_day=False)
    
    # For yesterday: wy_in = wy_in_station (no daily_in added)
    wy_in_station_mm = 7.0
    wy_in_station = np.round(wy_in_station_mm / 25.4, 2)
    assert payload["waterYearIN"] == pytest.approx(wy_in_station)


def test_format_asos_with_yesterday_flag(monkeypatch):
    """Test that format_asos correctly uses is_current_day flag for water year calc."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [50.0, 25.0])

    station_a = {
        "STID": "A1",
        "NAME": "ASOS Station",
        "ELEVATION": 20,
        "LATITUDE": 40.0,
        "LONGITUDE": -110.0,
        "OBSERVATIONS": {
            "air_temp_set_1": [5.0, 10.0, 8.0],
            "air_temp_high_6_hour_set_1": [12.0, 14.0, 16.0],
            "air_temp_low_6_hour_set_1": [1.0, 3.0, 5.0],
            "date_time": [
                "2024-01-01T22:00:00Z",
                "2024-01-02T02:00:00Z",
                "2024-01-02T04:00:00Z",
            ],
        },
    }

    station_b = {
        "STID": "B1",
        "OBSERVATIONS": {
            "precipitation": [
                {"total": 0.1, "last_report": "2024-01-01T23:00:00Z"},
                {"total": 0.3, "last_report": "2024-01-02T01:00:00Z"},
                {"total": 1.0, "last_report": "2024-01-02T03:00:00Z"},
            ]
        },
    }

    now = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    day_start, day_end = dp.climate_day_window(now, days_ago=1)
    
    # Test with is_current_day=False (yesterday)
    payload = dp.format_asos(station_a, station_b, day_start=day_start, day_end=day_end, now=now, is_current_day=False)
    
    # For yesterday: wy_in = 50.0 (no daily_in added)
    assert payload["waterYearIN"] == pytest.approx(50.0)


def test_compute_daily_temp_range_with_oso_hourmax_hourmin(monkeypatch):
    """Test that OSO hourMax/hourMin take priority over Synoptic data."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    times = [
        "2024-01-02T10:00:00Z",
        "2024-01-02T14:00:00Z",
    ]
    air_temp = [15.0, 18.0]  # Synoptic hourly temps
    
    # OSO provides different max/min
    hourMax = 25.0  # From OSO file
    hourMin = 5.0   # From OSO file
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    daily_max, daily_min = dp._compute_daily_temp_range(
        air_temp, times, None, None, hourMax=hourMax, hourMin=hourMin, 
        day_start=day_start, day_end=day_end
    )
    
    # Should use OSO values, not Synoptic
    assert daily_max == 25.0
    assert daily_min == 5.0


def test_parse_dt_parses_iso_format():
    """Test that _parse_dt correctly parses ISO datetime strings."""
    dt_str = "2024-01-02T12:30:45Z"
    result = dp._parse_dt(dt_str)
    
    assert result == datetime(2024, 1, 2, 12, 30, 45, tzinfo=timezone.utc)


def test_compute_daily_from_cumulative_with_grace_period(monkeypatch):
    """Test that grace period includes observations up to 30 min after day_end."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)

    cumulative_obs = [10.0, 12.5, 15.0]
    timestamps = [
        "2024-01-02T07:50:00Z",  # Before day_start
        "2024-01-03T07:55:00Z",  # Just before day_end
        "2024-01-03T08:15:00Z",  # 15 min after day_end (within grace period)
    ]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)

    daily = dp._compute_daily_from_cumulative(cumulative_obs, timestamps, day_start, day_end)
    # Should use the 08:15 observation (within grace period)
    assert daily == pytest.approx(5.0)  # 15.0 - 10.0


def test_parse_oso_file_parses_valid_file(tmp_path, monkeypatch):
    """Test that _parse_oso_file correctly parses a valid OSO file."""
    # Create mock OSO file
    oso_content = """SA 01021430
SFOOSOSFD
HI 65
LO 45
"""
    oso_file = tmp_path / "SFOOSOSFD"
    oso_file.write_text(oso_content)
    
    now = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)  # Within 2 hours of data
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    
    # HI 65F = 18.33C, LO 45F = 7.22C
    assert result[0] == pytest.approx((65 - 32) * 5 / 9, rel=0.01)
    assert result[1] == pytest.approx((45 - 32) * 5 / 9, rel=0.01)


def test_parse_oso_file_returns_none_if_data_too_old(tmp_path):
    """Test that _parse_oso_file returns None if data is >2 hours old."""
    # Create OSO file with timestamp more than 2 hours old
    oso_content = """SA 01021000
SFOOSOSFD
HI 65
LO 45
"""
    oso_file = tmp_path / "SFOOSOSFD"
    oso_file.write_text(oso_content)
    
    now = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)  # 5 hours after data
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    
    assert result == (None, None)


def test_parse_oso_file_handles_year_boundary(tmp_path):
    """Test that _parse_oso_file handles December to January boundary."""
    # Create OSO file with December timestamp that's recent
    oso_content = """SA 12311430
SFOOSOSFD
HI 50
LO 30
"""
    oso_file = tmp_path / "SFOOSOSFD"
    oso_file.write_text(oso_content)
    
    # Current time is January 1, 15:00, but within 2 hours of Dec 31 14:30
    # This should still be too old (>24 hours), so expect None
    now = datetime(2024, 1, 1, 15, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    
    # Data is >24 hours old, should return None
    assert result == (None, None)


def test_parse_oso_file_caches_and_updates_max_min(tmp_path, monkeypatch):
    """Test that OSO file parsing updates cache with max/min values."""
    import json
    
    # Create initial cache with lower values
    cache_file = tmp_path / "oso_cache.json"
    initial_cache = {
        "SFOC1": {
            "day": "2024-01-02",
            "max_hi": 15.0,
            "min_lo": 5.0,
            "last_update": "2024-01-02T10:00:00+00:00"
        }
    }
    cache_file.write_text(json.dumps(initial_cache))
    
    # Create OSO file with higher max and lower min
    oso_content = """SA 01021430
SFOOSOSFD
HI 75
LO 32
"""
    oso_file = tmp_path / "SFOOSOSFD"
    oso_file.write_text(oso_content)
    
    # Mock get_midnight to return consistent climate day
    midnight = datetime(2024, 1, 2, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    
    now = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    
    # Should return updated max/min
    hi_c = (75 - 32) * 5 / 9
    lo_c = (32 - 32) * 5 / 9
    assert result[0] == pytest.approx(max(15.0, hi_c), rel=0.01)
    assert result[1] == pytest.approx(min(5.0, lo_c), rel=0.01)


def test_parse_oso_file_returns_none_on_malformed_file(tmp_path):
    """Test that malformed OSO file returns None."""
    # Create malformed OSO file (missing HI/LO)
    oso_content = """SA 01021430
SFOOSOSFD
INVALID DATA
"""
    oso_file = tmp_path / "SFOOSOSFD"
    oso_file.write_text(oso_content)
    
    now = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    
    assert result == (None, None)


def test_parse_oso_file_returns_none_on_missing_timestamp(tmp_path):
    """Test that OSO file without timestamp returns None."""
    oso_content = """SFOOSOSFD
HI 65
LO 45
"""
    oso_file = tmp_path / "SFOOSOSFD"
    oso_file.write_text(oso_content)
    
    now = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
    result = dp._parse_oso_file("SFOC1", now, str(tmp_path), is_current_day=True)
    
    assert result == (None, None)


def test_unwrap_cumulative_handles_all_none():
    """Test unwrap_cumulative with all None values."""
    values = [None, None, None]
    result = dp.unwrap_cumulative(values)
    assert result == [None, None, None]


def test_unwrap_cumulative_handles_single_value():
    """Test unwrap_cumulative with single value."""
    values = [5.0]
    result = dp.unwrap_cumulative(values)
    assert result == [0.0]


def test_unwrap_cumulative_handles_multiple_resets():
    """Test unwrap_cumulative with multiple resets."""
    values = [1.0, 3.0, 1.0, 2.0, 0.5, 1.5]
    result = dp.unwrap_cumulative(values)
    # 0.0, 2.0, 2.0, 3.0, 3.0, 4.0
    assert result == [0.0, 2.0, 2.0, 3.0, 3.0, 4.0]


def test_get_precip_from_acis_handles_missing_data(monkeypatch):
    """Test _get_precip_from_acis handles missing/invalid ACIS data."""
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [])
    
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    wy_in, norm_in, pct = dp._get_precip_from_acis("TEST", now)
    
    assert wy_in == 9999
    assert norm_in == 9999
    assert pct == 9999


def test_get_precip_from_acis_handles_none_values(monkeypatch):
    """Test _get_precip_from_acis handles None values from ACIS."""
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [None, None])
    
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    wy_in, norm_in, pct = dp._get_precip_from_acis("TEST", now)
    
    assert wy_in == 9999
    assert norm_in == 9999
    assert pct == 9999


def test_get_precip_from_acis_handles_zero_norm(monkeypatch):
    """Test _get_precip_from_acis handles zero normal (division by zero)."""
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [5.0, 0.0])
    
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    wy_in, norm_in, pct = dp._get_precip_from_acis("TEST", now)
    
    assert wy_in == 5.0
    assert norm_in == 0.0
    assert pct == 9999


def test_get_precip_from_acis_calculates_percentage(monkeypatch):
    """Test _get_precip_from_acis correctly calculates percentage."""
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid, now: [7.5, 5.0])
    
    now = datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc)
    wy_in, norm_in, pct = dp._get_precip_from_acis("TEST", now)
    
    assert wy_in == 7.5
    assert norm_in == 5.0
    assert pct == 150


def test_compute_precip_from_hourly_handles_empty_data(monkeypatch):
    """Test _compute_precip_from_hourly with no data."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)
    
    result = dp._compute_precip_from_hourly([], [], day_start=day_start, day_end=day_end, period="daily")
    assert result is None


def test_compute_precip_from_hourly_handles_none_values(monkeypatch):
    """Test _compute_precip_from_hourly filters out None values."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    
    hourly = [None, 0.5, None, 1.0]
    timestamps = [
        "2024-01-02T10:00:00Z",
        "2024-01-02T12:00:00Z",
        "2024-01-02T14:00:00Z",
        "2024-01-02T16:00:00Z",
    ]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)
    
    result = dp._compute_precip_from_hourly(hourly, timestamps, day_start=day_start, day_end=day_end, period="daily")
    assert result == pytest.approx(1.5)


def test_compute_daily_from_cumulative_handles_empty_data():
    """Test _compute_daily_from_cumulative with no data."""
    day_start = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    day_end = day_start + __import__('datetime').timedelta(days=1)
    
    result = dp._compute_daily_from_cumulative([], [], day_start, day_end)
    assert result is None


def test_compute_daily_from_cumulative_handles_all_none_values():
    """Test _compute_daily_from_cumulative with all None cumulative values."""
    day_start = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    day_end = day_start + __import__('datetime').timedelta(days=1)
    
    timestamps = ["2024-01-02T10:00:00Z", "2024-01-02T12:00:00Z"]
    result = dp._compute_daily_from_cumulative([None, None], timestamps, day_start, day_end)
    assert result is None


def test_compute_daily_temp_range_returns_none_if_no_data():
    """Test _compute_daily_temp_range with no temperature data."""
    day_start = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    day_end = day_start + __import__('datetime').timedelta(days=1)
    
    result = dp._compute_daily_temp_range(None, [], None, None, day_start=day_start, day_end=day_end)
    assert result == (None, None)


def test_compute_daily_temp_range_filters_by_time_window(monkeypatch):
    """Test that _compute_daily_temp_range only uses data within the time window."""
    midnight = datetime(2024, 1, 2, 8, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda now=None: midnight)
    
    times = [
        "2024-01-02T07:00:00Z",  # Before window
        "2024-01-02T10:00:00Z",  # In window
        "2024-01-02T14:00:00Z",  # In window
        "2024-01-03T09:00:00Z",  # After window
    ]
    air_temp = [5.0, 10.0, 15.0, 20.0]
    
    day_start = midnight
    day_end = midnight + __import__('datetime').timedelta(days=1)
    
    daily_max, daily_min = dp._compute_daily_temp_range(air_temp, times, None, None, day_start=day_start, day_end=day_end)
    # Should only use 10.0 and 15.0
    assert daily_max == 15.0
    assert daily_min == 10.0


def test_build_yesterday_payload_uses_prefetched_data(monkeypatch):
    """Test that build_yesterday_payload uses already-fetched station data."""
    import bin.build_station_payloads as builder
    
    # Mock build_station_payload
    calls = []
    def fake_build(stations_a, stations_b=None, *, type, day_start, day_end, now, is_current_day):
        calls.append({'type': type, 'is_current_day': is_current_day})
        return [{"stid": f"{type}_{is_current_day}"}]
    
    monkeypatch.setattr(builder, "build_station_payload", fake_build)
    
    now = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)
    stationsA = [{"STID": "A1"}]
    stationsB = [{"STID": "B1"}]
    stationsC = [{"STID": "C1"}]
    
    payload = builder.build_yesterday_payload(stationsA, stationsB, stationsC, now)
    
    # Should have called build_station_payload twice with is_current_day=False
    assert len(calls) == 2
    assert all(call['is_current_day'] is False for call in calls)
    assert payload['data'] == [{"stid": "ASOS_False"}, {"stid": "HADS_False"}]
