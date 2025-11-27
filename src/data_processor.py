import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bin.fetch_xmacis_precip import fetch_xmacis_precip

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

def _get_precip_from_acis(stid: str) -> Tuple[float, float, int]:
    acis = fetch_xmacis_precip(stid)  # expected: acis[0] = wy_in, acis[1] = norm_in
    print(stid)

    def _safe_val(x: Any) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return 9999

    # Extract raw values defensively
    wy_raw  = acis[0] if len(acis) > 0 else None
    norm_raw = acis[1] if len(acis) > 1 else None

    wy_in   = _safe_val(wy_raw)
    norm_in = _safe_val(norm_raw)

    # Compute percent-of-normal with 9999 guard
    if wy_in == 9999 or norm_in == 9999 or norm_in == 0:
        pct = 9999
    else:
        pct = int((wy_in / norm_in) * 100)

    return wy_in, norm_in, pct

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

def _compute_precip_from_hourly(
    hourly: Any,
    fallback_time: Any,
    period: str = "daily",  # "daily" or "wateryear"
) -> Optional[float]:
    """Compute precip increment with low-value filtering.

    Args:
        hourly: Sequence of hourly amounts (not cumulative), or None.
        fallback_time: Corresponding timestamps (ISO UTC strings).
        period: "daily" => sum since local midnight;
                "wateryear" => sum over entire period.

    Logic:
      - Interpret `hourly` as hourly amounts.
      - Any obs < 0.254 is treated as 0.0.
      - For "daily", only sum values with timestamps >= local midnight.
      - For "wateryear", sum all values in the series.
    """
    if period not in ("daily", "wateryear"):
        raise ValueError(f"period must be 'daily' or 'wateryear', got {period!r}")

    entries: List[Tuple[str, Optional[float]]] = []

    if hourly is None:
        # No data; keep timestamps but vals are None
        for t in fallback_time:
            entries.append((t, None))
    else:
        for t, obs in zip(fallback_time, hourly):
            entries.append((t, obs))

    if not entries:
        return None

    # Sort by timestamp (assumed ISO strings like "YYYY-MM-DDTHH:MM:SSZ")
    entries.sort(key=lambda item: item[0])

    # Latest non-None hourly value (kept here in case you want it later)
    latest_val: Optional[float] = None
    for dt_str, val in reversed(entries):
        if val is not None:
            latest_val = val
            break

    # Only needed for daily mode
    mn = get_midnight() if period == "daily" else None

    total = 0.0
    has_any = False

    for dt_str, val in entries:
        # Parse to aware UTC datetime
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        if val is None:
            continue

        if period == "daily" and dt < mn:
            # Skip pre-midnight in daily mode
            continue

        has_any = True
        v = val if val >= 0.254 else 0.0  # filter small amounts
        if v > 0.0:
            total += v

    if not has_any:
        return None

    return total

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
    air_temp: Any,
    fallback_time: Any,
    maxT_6hr: Optional[Any] = None,
    minT_6hr: Optional[Any] = None,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute (daily_max, daily_min) since local midnight.

    - air_temp: hourly temps (list-like, may contain None).
    - maxT_6hr: optional 6-hr max temps (same grid, may be None).
    - minT_6hr: optional 6-hr min temps (same grid, may be None).

    If maxT_6hr/minT_6hr are None, the result is based only on air_temp.
    """

    if air_temp is None:
        return None, None

    mn = get_midnight()  # timezone-aware UTC datetime

    def _iter_since_midnight(values: Any):
        if values is None:
            return
        for t, v in zip(fallback_time, values):
            if v is None:
                continue
            dt = datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if dt >= mn:
                yield v

    # Hourly-based max/min since midnight
    hourly_vals = list(_iter_since_midnight(air_temp))
    if not hourly_vals:
        return None, None

    hourly_max: Optional[float] = max(hourly_vals)
    hourly_min: Optional[float] = min(hourly_vals)

    # Optional 6-hr maxima/minima
    max6_vals = list(_iter_since_midnight(maxT_6hr)) if maxT_6hr is not None else []
    min6_vals = list(_iter_since_midnight(minT_6hr)) if minT_6hr is not None else []

    max6: Optional[float] = max(max6_vals) if max6_vals else None
    min6: Optional[float] = min(min6_vals) if min6_vals else None

    def _pick_max(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None:
            return b
        if b is None:
            return a
        return max(a, b)

    def _pick_min(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None:
            return b
        if b is None:
            return a
        return min(a, b)

    daily_max = _pick_max(hourly_max, max6)
    daily_min = _pick_min(hourly_min, min6)

    if daily_max is None or daily_min is None:
        return None, None

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
    daily_in = mm_to_in(daily_accum)

    stid = station.get("STID", {})
    wy_in, norm_in, pct = _get_precip_from_acis(stid)

    wy_in = mm_to_in(wy_accum[-1]) # Comment out to use ACIS Precip Accum

    return {
        "stid": station.get("STID"),
        "name": station.get("NAME"),
        "elevationFT": station.get("ELEVATION"),
        "latitude": station.get("LATITUDE"),
        "longitude": station.get("LONGITUDE"),
        "dateTime": dt_latest,
        "airTempF": currentF,
        "dailyMaxF": maxF,
        "dailyMinF": minF,
        "dailyAccumIN": daily_in,
        "waterYearIN": wy_in,
        "waterYearNormIN": norm_in,
        "percentOfNorm": pct,
    }

def format_asos(station_a: Dict[str, Any], station_b: Optional[Dict[str, Any]]) -> Dict[str, Any]:

    ### temperature info from station_a ###
    observations = station_a.get("OBSERVATIONS", {})

    air_temp = observations.get("air_temp_set_1")
    maxT_6hr = observations.get("air_temp_high_6_hour_set_1")
    minT_6hr = observations.get("air_temp_low_6_hour_set_1")

    date_time = observations.get("date_time")
    dt_latest = date_time[-1]

    daily_maxT, daily_minT = _compute_daily_temp_range(air_temp, date_time, maxT_6hr, minT_6hr)

    maxF = c_to_f(daily_maxT)
    minF = c_to_f(daily_minT)
    currentF = c_to_f(air_temp[-1])

    ### precip info from station_b ###
    hourly = [
        entry.get("total")
        for entry in station_b.get("OBSERVATIONS", {}).get("precipitation", [])
    ]

    date_time_b = [
        entry.get("last_report")
        for entry in station_b.get("OBSERVATIONS", {}).get("precipitation", [])
    ]

    daily_accum = _compute_precip_from_hourly(hourly, date_time_b, "daily")

    daily_in = mm_to_in(daily_accum)

    if station_b is not None:
        stid = station_b.get("STID", [])
        wy_in, norm_in, pct = _get_precip_from_acis(stid)

    return {
            "stid": station_a.get("STID"),
            "name": station_a.get("NAME"),
            "elevationFT": station_a.get("ELEVATION"),
            "latitude": station_a.get("LATITUDE"),
            "longitude": station_a.get("LONGITUDE"),
            "dateTime": dt_latest,
            "airTempF": currentF,
            "dailyMaxF": maxF,
            "dailyMinF": minF,
            "dailyAccumIN": daily_in,
            "waterYearIN": wy_in,
            "waterYearNormIN": norm_in,
            "percentOfNorm": pct,
        }

def build_station_payload(
    stations_a: List[Dict[str, Any]],
    stations_b: Optional[List[Dict[str, Any]]] = None,
    *,
    type: str
) -> List[Dict[str, Any]]:
    """Transform a list of station records into a simplified structure."""
    if type is None:
        raise ValueError("Type is required.")

    if type == "HADS":
        return [format_hads(station) for station in stations_a]
    
    if type == "ASOS":
        if stations_b is None:
            raise ValueError("stations_b is required when type='ASOS'.")

        if len(stations_a) != len(stations_b):
            raise ValueError(
                f"stations_a and stations_b must be same length for ASOS "
                f"(got {len(stations_a)} and {len(stations_b)})"
            )

        return [
            format_asos(station_a, station_b)
            for station_a, station_b in zip(stations_a, stations_b)
        ]

    raise ValueError(f"Unknown type: {type!r}")