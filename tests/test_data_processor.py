import pytest
from datetime import datetime, timezone

from src.data_processor import (
    _extract_single_value,
    _parse_datetime,
    _temp_extremes_since_midnight,
    build_station_payload,
    format_station_data,
)


def test_extract_single_value_handles_various_inputs():
    assert _extract_single_value([1.5, 2]) == 1.5
    assert _extract_single_value([]) is None
    assert _extract_single_value("3.5") == 3.5
    assert _extract_single_value("not-a-number") is None
    assert _extract_single_value(None) is None


def test_parse_datetime_parses_strings_and_applies_timezone():
    dt = _parse_datetime("2024-01-01T12:00:00Z", "UTC")

    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.hour == 12
    assert dt.utcoffset() == timezone.utc.utcoffset(dt)


def test_parse_datetime_falls_back_to_utc_on_invalid_timezone():
    naive = datetime(2024, 1, 1, 6, 30)
    dt = _parse_datetime(naive, "Not/AZone")

    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 6
    assert dt.minute == 30


def test_parse_datetime_returns_none_for_invalid_values():
    assert _parse_datetime("not-a-date", "UTC") is None
    assert _parse_datetime(None, "UTC") is None


def test_temp_extremes_since_midnight_ignores_invalid_records():
    observation_times = [
        "2024-06-01T01:00:00Z",
        "invalid",
        None,
        "2024-06-01T03:00:00Z",
    ]
    temps = [72.1, 999, "bad", 75.5]

    min_temp, max_temp = _temp_extremes_since_midnight(
        observation_times, temps, "UTC"
    )

    assert min_temp == 72.1
    assert max_temp == 75.5


def test_temp_extremes_since_midnight_truncates_to_pairs():
    times = ["2024-06-01T01:00:00Z", "2024-06-01T02:00:00Z"]
    temps = [70.0]

    min_temp, max_temp = _temp_extremes_since_midnight(times, temps, "UTC")

    assert (min_temp, max_temp) == (70.0, 70.0)


def test_format_station_data_returns_simplified_payload():
    station = {
        "STID": "TEST1",
        "NAME": "Test Station",
        "ELEVATION": 50,
        "LATITUDE": 37.1,
        "LONGITUDE": -122.1,
        "TIMEZONE": "UTC",
        "OBSERVATIONS": {
            "air_temp_value_1": [70.1, 68.0, 71.5],
            "relative_humidity_value_1": [40],
            "precip_accum_one_hour_value_1": [0.0],
            "date_time": [
                "2024-07-01T10:00:00Z",
                "2024-07-01T11:00:00Z",
                "2024-07-01T12:00:00Z",
            ],
        },
    }

    result = format_station_data(station)

    assert result["stid"] == "TEST1"
    assert result["name"] == "Test Station"
    assert result["elevation"] == 50
    assert result["latitude"] == 37.1
    assert result["longitude"] == -122.1
    assert result["date_time"] == "2024-07-01T10:00:00Z"
    assert result["air_temp_value_1"] == pytest.approx(70.1)
    assert result["relative_humidity_value_1"] == pytest.approx(40)
    assert result["precip_accum_one_hour_value_1"] == pytest.approx(0.0)
    assert result["min_temp_since_midnight"] == pytest.approx(68.0)
    assert result["max_temp_since_midnight"] == pytest.approx(71.5)


def test_build_station_payload_formats_each_station():
    stations = [
        {
            "STID": "ONE",
            "TIMEZONE": "UTC",
            "OBSERVATIONS": {
                "air_temp_value_1": [60.0],
                "relative_humidity_value_1": [30],
                "precip_accum_one_hour_value_1": [0.1],
                "date_time": ["2024-07-02T00:00:00Z"],
            },
        },
        {
            "STID": "TWO",
            "TIMEZONE": "UTC",
            "OBSERVATIONS": {
                "air_temp_value_1": 65.0,
                "relative_humidity_value_1": 50,
                "precip_accum_one_hour_value_1": None,
                "date_time": "2024-07-02T01:00:00Z",
            },
        },
    ]

    payload = build_station_payload(stations)

    assert [item["stid"] for item in payload] == ["ONE", "TWO"]
    assert payload[0]["air_temp_value_1"] == pytest.approx(60.0)
    assert payload[1]["air_temp_value_1"] == pytest.approx(65.0)
    assert payload[1]["relative_humidity_value_1"] == pytest.approx(50)
    assert payload[1]["precip_accum_one_hour_value_1"] is None
