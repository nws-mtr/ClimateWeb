from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for older Python
    from backports.zoneinfo import ZoneInfo  # type: ignore


def _extract_single_value(values: Any) -> Optional[float]:
    """Extract a single numeric value from API observation arrays."""
    if values is None:
        return None
    if isinstance(values, list):
        return values[0] if values else None
    try:
        return float(values)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any, timezone_name: str) -> Optional[datetime]:
    """Parse a datetime string into an aware datetime in the target timezone."""
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        cleaned = value.replace("Z", "+00:00") if value.endswith("Z") else value
        try:
            dt = datetime.fromisoformat(cleaned)
        except ValueError:
            return None
    else:
        return None

    try:
        tzinfo = ZoneInfo(timezone_name)
    except Exception:
        tzinfo = timezone.utc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tzinfo)
    else:
        dt = dt.astimezone(tzinfo)

    return dt


def _midnight_for_timezone(reference: datetime) -> datetime:
    """Return the midnight boundary for the provided timezone-aware datetime."""
    localized = reference.astimezone(reference.tzinfo or timezone.utc)
    return localized.replace(hour=0, minute=0, second=0, microsecond=0)


def _temp_extremes_since_midnight(
    observation_times: Iterable[Any],
    temps: Iterable[Any],
    timezone_name: str,
) -> Tuple[Optional[float], Optional[float]]:
    """Compute min and max temperatures since local midnight."""

    # Pair observations with timestamps; unequal lengths will be truncated by zip.
    min_temp: Optional[float] = None
    max_temp: Optional[float] = None

    for raw_time, raw_temp in zip(observation_times, temps):
        dt = _parse_datetime(raw_time, timezone_name)
        temp_value = _extract_single_value(raw_temp)

        if dt is None or temp_value is None:
            continue

        midnight = _midnight_for_timezone(dt)
        if dt < midnight:
            continue

        if min_temp is None or temp_value < min_temp:
            min_temp = temp_value
        if max_temp is None or temp_value > max_temp:
            max_temp = temp_value

    return min_temp, max_temp


def format_station_data(station: Dict[str, Any]) -> Dict[str, Any]:
    """Format station payload into a simplified dictionary."""
    observations = station.get("OBSERVATIONS", {})
    timezone_name = station.get("TIMEZONE", "UTC")

    air_temp = _extract_single_value(observations.get("air_temp_value_1"))
    rel_humidity = _extract_single_value(observations.get("relative_humidity_value_1"))
    precip_one_hour = _extract_single_value(
        observations.get("precip_accum_one_hour_value_1")
    )
    date_time = observations.get("date_time", [None])
    date_time_value = date_time[0] if isinstance(date_time, list) else date_time

    observation_times = date_time if isinstance(date_time, list) else [date_time]
    temp_series = observations.get("air_temp_value_1", [])
    if not isinstance(temp_series, list):
        temp_series = [temp_series]

    min_temp, max_temp = _temp_extremes_since_midnight(
        observation_times, temp_series, timezone_name
    )

    return {
        "stid": station.get("STID"),
        "name": station.get("NAME"),
        "elevation": station.get("ELEVATION"),
        "latitude": station.get("LATITUDE"),
        "longitude": station.get("LONGITUDE"),
        "date_time": date_time_value,
        "air_temp_value_1": air_temp,
        "relative_humidity_value_1": rel_humidity,
        "precip_accum_one_hour_value_1": precip_one_hour,
        "min_temp_since_midnight": min_temp,
        "max_temp_since_midnight": max_temp,
    }


def build_station_payload(stations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform a list of station records into a simplified structure."""
    return [format_station_data(station) for station in stations]
