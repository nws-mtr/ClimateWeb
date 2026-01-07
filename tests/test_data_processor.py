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
    monkeypatch.setattr(dp, "_parse_oso_file", lambda stid, now, home_dir: (None, None))

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
