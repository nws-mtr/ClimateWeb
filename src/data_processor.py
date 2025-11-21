import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback for older Python
    from backports.zoneinfo import ZoneInfo  # type: ignore

def mm_to_in(mm):
    if mm is None:
        return None
    value = np.round(mm / 25.4, decimals=2)
    return value

def c_to_f(c):
    if c is None:
        return None
    return (c * 9/5) + 32

def get_midnight():
    now = datetime.now(timezone.utc)
    midnight = now.replace(hour=8, minute=0, second=0, microsecond=0)

    if now < midnight:
        midnight -= timedelta(days=1)

    return midnight

def _compute_daily_from_cumulative(
    cumulative_obs: Any, fallback_time: Any
) -> Tuple[Optional[float], Optional[float]]:
    """Compute (latest cumulative value, daily increment since local midnight)."""
    entries: List[Tuple[datetime, float]] = []
        
    if cumulative_obs is None:
        for t in fallback_time:
            entries.append((t, None))
    else:
        for t, obs in zip(fallback_time, cumulative_obs):
            entries.append((t, obs))

    if not entries:
        return None, None

    entries.sort(key=lambda item: item[0])
    latest_dt, latest_val = entries[-1]
    mn = get_midnight()

    baseline_val: Optional[float] = None
    for dt_, val in reversed(entries):
        dt = datetime.strptime(dt_, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if dt <= mn:
            baseline_val = val
            break

    if baseline_val is None:
        return latest_val, None

    daily = latest_val - baseline_val
    if daily < 0:
        return latest_val, None

    return latest_val, daily

def _compute_daily_temp_range(
    air_temp: Any, fallback_time: Any,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute (max_temp, min_temp) as the max and min air temperature
    since local midnight (using get_midnight(), which is assumed to be UTC).
    """
    entries: List[Tuple[datetime, float]] = []

    if air_temp is None:
        return None, None

    for t, temp in zip(fallback_time, air_temp):
        if temp is None:
            continue
        dt = datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        entries.append((dt, temp))

    if not entries:
        return None, None

    entries.sort(key=lambda item: item[0])

    mn = get_midnight()

    temps_since_midnight = [val for dt, val in entries if dt >= mn]

    if not temps_since_midnight:
        return None, None

    daily_max = max(temps_since_midnight)
    daily_min = min(temps_since_midnight)

    return daily_max, daily_min

def format_station_data(station: Dict[str, Any]) -> Dict[str, Any]:
    """Format station payload into a simplified dictionary."""
    observations = station.get("OBSERVATIONS", {})
    # timezone_name = station.get("TIMEZONE", "UTC")
    timezone_name = timezone.utc

    air_temp = observations.get("air_temp_set_1")
    rel_humidity = observations.get("relative_humidity_set_1")
    precip_one_hour = observations.get("precip_accum_one_hour_set_1")
    precip_accum = observations.get("precip_accum_set_1")

    date_time = observations.get("date_time")
    dt_latest = date_time[-1]

    daily_maxT, daily_minT = _compute_daily_temp_range(air_temp, date_time)

    maxF = c_to_f(daily_maxT)
    minF = c_to_f(daily_minT)
    currentF = c_to_f(air_temp[-1])

    if station.get("STID")[-1] == "1":
        wy_accum: Optional[float] = None
        daily_accum: Optional[float] = None
        # precip_accum_value = _extract_single_value(precip_accum_raw)
        wy_accum, daily_accum = _compute_daily_from_cumulative(
            precip_accum, date_time)
        
        wy_in = mm_to_in(wy_accum)
        daily_in = mm_to_in(daily_accum)

        return {
            "stid": station.get("STID"),
            "name": station.get("NAME"),
            "elevation": station.get("ELEVATION"),
            "latitude": station.get("LATITUDE"),
            "longitude": station.get("LONGITUDE"),
            "date_time": dt_latest,
            "air_temp": currentF,
            "daily_maxT": maxF,
            "daily_minT": minF,
            "daily_accum": daily_in,
            "wy_accum": wy_in,
        }
    
    else:

        return {
            "stid": station.get("STID"),
            "name": station.get("NAME"),
            "elevation": station.get("ELEVATION"),
            "latitude": station.get("LATITUDE"),
            "longitude": station.get("LONGITUDE"),
            "date_time": dt_latest,
            "air_temp": currentF,
            "daily_maxT": maxF,
            "daily_minT": minF,
            "relative_humidity": rel_humidity[-1],
            "precip_accum_one_hour": precip_one_hour[-1],
            # "daily_accum": daily_accum,
            # "wy_accum": wy_accum,
        }  


def build_station_payload(stations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Transform a list of station records into a simplified structure."""
    return [format_station_data(station) for station in stations]
