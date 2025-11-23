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
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)

    cumulative_obs = [10.0, 12.5]
    timestamps = ["2024-01-01T23:00:00Z", "2024-01-02T06:00:00Z"]

    daily = dp._compute_daily_from_cumulative(cumulative_obs, timestamps)
    assert daily == pytest.approx(2.5)


def test_compute_daily_from_cumulative_returns_none_on_reset(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)

    cumulative_obs = [5.0, 1.0]
    timestamps = ["2024-01-01T23:00:00Z", "2024-01-02T02:00:00Z"]

    assert dp._compute_daily_from_cumulative(cumulative_obs, timestamps) is None


def test_compute_precip_from_hourly_daily_filters_and_sums(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)

    hourly = [0.1, 0.3, 0.5, 1.0]
    timestamps = [
        "2024-01-01T22:00:00Z",  # before midnight -> ignored
        "2024-01-02T00:30:00Z",  # below threshold -> 0
        "2024-01-02T02:00:00Z",  # counted
        "2024-01-02T04:00:00Z",  # counted
    ]

    total = dp._compute_precip_from_hourly(hourly, timestamps, "daily")
    assert total == pytest.approx(1.8)


def test_compute_precip_from_hourly_wateryear_includes_all(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)

    hourly = [0.1, 0.3, 0.5, 1.0]
    timestamps = [
        "2024-01-01T22:00:00Z",
        "2024-01-02T00:30:00Z",
        "2024-01-02T02:00:00Z",
        "2024-01-02T04:00:00Z",
    ]

    total = dp._compute_precip_from_hourly(hourly, timestamps, "wateryear")
    assert total == pytest.approx(1.8)


def test_compute_precip_from_hourly_validates_period():
    with pytest.raises(ValueError):
        dp._compute_precip_from_hourly([], [], "invalid")


def test_unwrap_cumulative_handles_resets_and_missing():
    values = [None, 1.0, 2.0, 1.5, 3.0]
    result = dp.unwrap_cumulative(values)
    assert result == [None, 0.0, 1.0, 1.0, 2.5]


def test_compute_daily_temp_range_uses_hourly_and_6hr(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)

    times = [
        "2024-01-01T22:00:00Z",
        "2024-01-02T01:00:00Z",
        "2024-01-02T05:00:00Z",
    ]
    air_temp = [5.0, 10.0, 8.0]
    max6 = [12.0, 15.0, 20.0]
    min6 = [2.0, 4.0, 6.0]

    daily_max, daily_min = dp._compute_daily_temp_range(air_temp, times, max6, min6)
    assert daily_max == pytest.approx(20.0)
    assert daily_min == pytest.approx(4.0)


def test_format_hads_builds_payload(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid: [100.0, 80.0])

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

    payload = dp.format_hads(station)

    assert payload["stid"] == "H1"
    assert payload["name"] == "HADS Station"
    assert payload["airTempF"] == 57  # 14C -> 57F
    assert payload["dailyMaxF"] == 57  # 14C -> 57F
    assert payload["dailyMinF"] == 54  # 12C -> 54F
    assert payload["dailyAccumIN"] == pytest.approx(np.round(7 / 25.4, 2))
    assert payload["waterYearIN"] == pytest.approx(np.round(7 / 25.4, 2))
    assert payload["waterYearNormIN"] == 80.0
    assert payload["percentOfNorm"] == 125


def test_format_asos_combines_station_data(monkeypatch):
    midnight = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(dp, "get_midnight", lambda: midnight)
    monkeypatch.setattr(dp, "fetch_xmacis_precip", lambda stid: [50.0, 25.0])

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

    payload = dp.format_asos(station_a, station_b)

    assert payload["stid"] == "A1"
    assert payload["name"] == "ASOS Station"
    assert payload["airTempF"] == dp.c_to_f(8.0)
    assert payload["dailyMaxF"] == dp.c_to_f(16.0)
    assert payload["dailyMinF"] == dp.c_to_f(3.0)
    assert payload["dailyAccumIN"] == pytest.approx(np.round((0.3 + 1.0) / 25.4, 2))
    assert payload["waterYearIN"] == 50.0
    assert payload["waterYearNormIN"] == 25.0
    assert payload["percentOfNorm"] == 200


def test_build_station_payload_validates_input(monkeypatch):
    with pytest.raises(ValueError):
        dp.build_station_payload([], type=None)

    with pytest.raises(ValueError):
        dp.build_station_payload([], type="UNKNOWN")

    with pytest.raises(ValueError):
        dp.build_station_payload([{}], [{}, {}], type="ASOS")

    monkeypatch.setattr(dp, "format_hads", lambda station: {"stid": station["STID"]})
    result = dp.build_station_payload([{"STID": "H1"}], type="HADS")
    assert result == [{"stid": "H1"}]

    monkeypatch.setattr(dp, "format_asos", lambda a, b: {"stid": a["STID"] + b["STID"]})
    result = dp.build_station_payload([{"STID": "A"}], [{"STID": "B"}], type="ASOS")
    assert result == [{"stid": "AB"}]
