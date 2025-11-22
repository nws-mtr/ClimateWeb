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
    return np.round(mm / 25.4, decimals=2)

def c_to_f(c):
    if c is None:
        return None
    return int(round((c * 9/5) + 32))

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
        return None

    daily = latest_val - baseline_val
    if daily < 0:
        return None

    return daily

def _compute_daily_from_hourly(
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
        return None

    daily = latest_val - baseline_val
    if daily < 0:
        return None

    return daily

def unwrap_cumulative(values: Iterable[Optional[float]]) -> List[Optional[float]]:
    """
    Convert a possibly-resetting cumulative series into a monotonic one,
    assuming the true accumulation starts at 0 at the first value.

    Rules:
      - First non-None value => treated as 0 accumulated.
      - Only positive deltas (current > previous) add to the total.
      - Zero or negative deltas contribute 0 (plateaus/resets).
      - Output is a monotonic, non-decreasing series starting at 0.
    """
    out: List[Optional[float]] = []
    prev: Optional[float] = None
    total: float = 0.0

    for v in values:
        if v is None:
            # Preserve missing values; do not advance the state.
            out.append(None)
            continue

        if prev is None:
            # First valid value initializes the baseline; no increment yet.
            prev = v
            out.append(0.0)
            continue

        if v > prev:
            total += (v - prev)
        # else: v <= prev -> reset or flat; no change in total

        out.append(total)
        prev = v

    return out


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

def format_hads(station: Dict[str, Any]) -> Dict[str, Any]:
    observations = station.get("OBSERVATIONS", {})
    
    air_temp = observations.get("air_temp_set_1")
    precip_accum = observations.get("precip_accum_set_1")
    
    date_time = observations.get("date_time")
    dt_latest = date_time[-1]

    daily_maxT, daily_minT = _compute_daily_temp_range(air_temp, date_time)

    maxF = c_to_f(daily_maxT)
    minF = c_to_f(daily_minT)
    currentF = c_to_f(air_temp[-1])

    wy_accum: Optional[float] = None
    daily_accum: Optional[float] = None
    daily_accum = _compute_daily_from_cumulative(
        precip_accum, date_time)
    
    wy_accum = unwrap_cumulative(precip_accum)
    wy_in = mm_to_in(wy_accum[-1])
    daily_in = mm_to_in(daily_accum)

    return {
        "stid": station.get("STID"),
        "name": station.get("NAME"),
        "elevation": station.get("ELEVATION"),
        "latitude": station.get("LATITUDE"),
        "longitude": station.get("LONGITUDE"),
        "dateTime": dt_latest,
        "airTempF": currentF,
        "dailyMaxF": maxF,
        "dailyMinF": minF,
        "dailyAccumIN": daily_in,
        "waterYearIN": wy_in,
    }

def format_asos_latest(station: Dict[str, Any]) -> Dict[str, Any]:
    observations = station.get("OBSERVATIONS", {})

    air_temp = observations.get("air_temp_set_1")
    maxT_6hr = observations.get("air_temp_high_6_hour_set_1")
    minT_6hr = observations.get("air_temp_low_6_hour_set_1")

    date_time = observations.get("date_time")
    dt_latest = date_time[-1]

    daily_maxT, daily_minT = _compute_daily_temp_range(air_temp, date_time)

    maxF = c_to_f(daily_maxT)
    minF = c_to_f(daily_minT)
    currentF = c_to_f(air_temp[-1])

    return {
            "stid": station.get("STID"),
            "name": station.get("NAME"),
            "elevation": station.get("ELEVATION"),
            "latitude": station.get("LATITUDE"),
            "longitude": station.get("LONGITUDE"),
            "dateTime": dt_latest,
            "airTempF": currentF,
            "dailyMaxF": maxF,
            "dailyMinF": minF,
        }

def format_asos_precip(station: Dict[str, Any]) -> Dict[str, Any]:
    observations = station.get("OBSERVATIONS", {})

    ### LEFT OFF HERE. COMPUTE DAILY AND WY FROM HOURLY. ###
    
    totals = [
        entry.get("total")
        for entry in station.get("OBSERVATIONS", {}).get("precipitation", [])
    ]

    date_time = [
        entry.get("last_report")
        for entry in station.get("OBSERVATIONS", {}).get("precipitation", [])
    ]

    

    # hour_accum = mm_to_in(precip_one_hour[-1])

    return {
        # "precip_accum_one_hour": hour_accum,
        # "daily_accum": daily_accum,
        # "wy_accum": wy_accum,
    }  


def build_station_payload(
    stations: List[Dict[str, Any]],
    *,
    type: str
) -> List[Dict[str, Any]]:
    """Transform a list of station records into a simplified structure."""
    if type is None:
        raise ValueError("Type is required.")
    if type == "ASOS_latest":
        return [format_asos_latest(station) for station in stations]
    if type == "ASOS_precip":
        return [format_asos_precip(station) for station in stations]
    if type == "HADS":
        return [format_hads(station) for station in stations]