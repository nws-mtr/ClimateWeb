from typing import Any, Dict, List, Optional


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


def format_station_data(station: Dict[str, Any]) -> Dict[str, Any]:
    """Format station payload into a simplified dictionary."""
    observations = station.get("OBSERVATIONS", {})

    air_temp = _extract_single_value(observations.get("air_temp_value_1"))
    rel_humidity = _extract_single_value(observations.get("relative_humidity_value_1"))
    date_time = observations.get("date_time", [None])
    date_time_value = date_time[0] if isinstance(date_time, list) else date_time

    return {
        "stid": station.get("STID"),
        "name": station.get("NAME"),
        "elevation": station.get("ELEVATION"),
        "latitude": station.get("LATITUDE"),
        "longitude": station.get("LONGITUDE"),
        "date_time": date_time_value,
        "air_temp_value_1": air_temp,
        "relative_humidity_value_1": rel_humidity,
    }


def build_station_payload(stations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform a list of station records into a simplified structure."""
    return [format_station_data(station) for station in stations]
