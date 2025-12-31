import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import os

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


def _parse_dt(dt_str: str) -> datetime:
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def get_midnight(now: datetime | None = None) -> datetime:
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    midnight = now.replace(hour=8, minute=0, second=0, microsecond=0)

    if now < midnight:
        midnight -= timedelta(days=1)

    return midnight


def climate_day_window(now: datetime, days_ago: int = 0) -> Tuple[datetime, datetime]:
    reference = now - timedelta(days=days_ago)
    start = get_midnight(reference)
    end = start + timedelta(days=1)
    return start, end


def _get_precip_from_acis(stid: str, now: datetime) -> Tuple[float, float, int]:
    acis = fetch_xmacis_precip(stid, now=now)  # expected: acis[0] = wy_in, acis[1] = norm_in
    print(stid)

    def _safe_val(x: Any) -> float:
        try:
            return float(x)
        except (TypeError, ValueError):
            return 9999

    wy_raw  = acis[0] if len(acis) > 0 else None
    norm_raw = acis[1] if len(acis) > 1 else None

    wy_in   = _safe_val(wy_raw)
    norm_in = _safe_val(norm_raw)

    if wy_in == 9999 or norm_in == 9999 or norm_in == 0:
        pct = 9999
    else:
        pct = int((wy_in / norm_in) * 100)

    return wy_in, norm_in, pct


def _compute_daily_from_cumulative(
    cumulative_obs: Any,
    fallback_time: Any,
    day_start: datetime,
    day_end: datetime,
) -> Optional[float]:
    entries: List[Tuple[str, Optional[float]]] = []

    if cumulative_obs is None:
        entries = [(t, None) for t in fallback_time or []]
    else:
        entries = list(zip(fallback_time or [], cumulative_obs))

    if not entries:
        return None

    def _latest_before(target: datetime) -> Optional[float]:
        for dt_str, val in reversed(entries):
            dt = _parse_dt(dt_str)
            if dt <= target:
                return val
        return None

    latest_val = _latest_before(day_end)
    baseline_val = _latest_before(day_start)

    if latest_val is None or baseline_val is None:
        return None

    try:
        daily = float(latest_val) - float(baseline_val)
    except (TypeError, ValueError):
        return None

    if daily < 0:
        return None

    return daily


def _compute_precip_from_hourly(
    hourly: Any,
    fallback_time: Any,
    *,
    day_start: datetime,
    day_end: datetime,
    period: str = "daily",
) -> Optional[float]:
    if period not in ("daily", "wateryear"):
        raise ValueError(f"period must be 'daily' or 'wateryear', got {period!r}")

    entries: List[Tuple[str, Optional[float]]] = []

    if hourly is None:
        entries = [(t, None) for t in fallback_time or []]
    else:
        entries = list(zip(fallback_time or [], hourly))

    if not entries:
        return None

    entries.sort(key=lambda item: item[0])

    total = 0.0
    has_any = False

    for dt_str, val in entries:
        dt = _parse_dt(dt_str)

        if val is None:
            continue

        if period == "daily" and (dt < day_start or dt >= day_end):
            continue

        has_any = True
        v = val if val >= 0.254 else 0.0
        if v > 0.0:
            total += v

    if not has_any:
        return None

    return total


def unwrap_cumulative(values: Iterable[Optional[float]]) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    prev: Optional[float] = None
    total: float = 0.0

    for v in values:
        if v is None:
            out.append(None)
            continue

        if prev is None:
            prev = v
            out.append(0.0)
            continue

        if v > prev:
            total += (v - prev)

        out.append(total)
        prev = v

    return out


def _compute_daily_temp_range(
    air_temp: Any,
    fallback_time: Any,
    maxT_6hr: Optional[Any] = None,
    minT_6hr: Optional[Any] = None,
    hourMax: Optional[float] = None,
    hourMin: Optional[float] = None,
    *,
    day_start: datetime,
    day_end: datetime,
) -> Tuple[Optional[float], Optional[float]]:
    if air_temp is None:
        return None, None

    def _iter_in_window(values: Any):
        if values is None:
            return
        for t, v in zip(fallback_time or [], values):
            if v is None:
                continue
            dt = _parse_dt(t)
            if dt >= day_start and dt < day_end:
                yield v

    hourly_vals = list(_iter_in_window(air_temp))
    if not hourly_vals:
        return None, None

    hourly_max: Optional[float] = max(hourly_vals)
    hourly_min: Optional[float] = min(hourly_vals)

    max6_vals = list(_iter_in_window(maxT_6hr)) if maxT_6hr is not None else []
    min6_vals = list(_iter_in_window(minT_6hr)) if minT_6hr is not None else []

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

    # If hourMax and hourMin are provided from OSO, use them as the primary source
    if hourMax is not None and hourMin is not None:
        daily_max = hourMax
        daily_min = hourMin
    else:
        daily_max = _pick_max(hourly_max, max6)
        daily_min = _pick_min(hourly_min, min6)

    if daily_max is None or daily_min is None:
        return None, None

    return daily_max, daily_min


def _load_oso_cache(cache_file: str) -> Dict[str, Any]:
    """Load OSO cache from JSON file."""
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {}


def _save_oso_cache(cache_file: str, cache: Dict[str, Any]) -> None:
    """Save OSO cache to JSON file."""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache, f, indent=2)
    except IOError:
        pass


def _parse_oso_file(stid: str, now: datetime, home_dir: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse OSO text file to extract accumulated daily high and low temps.
    Tracks hourly HI/LO values and returns the max HI and min LO since 0800 UTC.
    Returns (hourMax, hourMin) in Celsius if data is fresh (<2 hours old), otherwise (None, None).
    """
    # Map station IDs to OSO file suffixes
    stn_map = {
        'SFOC1': 'SFD',
        'RWCC1': 'RWC',
        'SARC1': 'SRF',
        'OAMC1': 'OKL',
    }
    
    stn = stn_map.get(stid)
    if stn is None:
        return None, None
    
    oso_file = f"{home_dir}\\SFOOSO{stn}"
    cache_file = f"{home_dir}\\oso_cache.json"
    
    try:
        with open(oso_file, 'r') as f:
            content = f.read()
        
        # Parse the time from the format "SA MMDDhhmm"
        import re
        time_match = re.search(r'SA (\d{8})', content)
        if not time_match:
            return None, None
        
        time_str = time_match.group(1)
        
        # Parse MMDDhhmm UTC
        month = int(time_str[0:2])
        day = int(time_str[2:4])
        hour = int(time_str[4:6])
        minute = int(time_str[6:8])
        
        # Construct datetime - use current year, handle year boundary
        year = now.year
        if now.month == 1 and month == 12:
            year -= 1
        
        oso_time = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        
        # Check if data is more than 2 hours old
        time_diff = now - oso_time
        if time_diff > timedelta(hours=2):
            return None, None
        
        # Extract HI and LO values (in Fahrenheit)
        hi_match = re.search(r'HI\s+(\d+)', content)
        lo_match = re.search(r'LO\s+(\d+)', content)
        
        if not hi_match or not lo_match:
            return None, None
        
        hi_f = float(hi_match.group(1))
        lo_f = float(lo_match.group(1))
        
        # Convert to Celsius for internal use (will be converted back to F in format_hads)
        hi_c = (hi_f - 32) * 5 / 9
        lo_c = (lo_f - 32) * 5 / 9
        
        # Load cache and get current climate day boundaries
        cache = _load_oso_cache(cache_file)
        day_start = get_midnight(now)
        day_key = day_start.strftime("%Y-%m-%d")
        
        # Initialize or get station's daily cache
        if stid not in cache:
            cache[stid] = {}
        
        # Reset cache if we're on a new climate day
        if 'day' not in cache[stid] or cache[stid]['day'] != day_key:
            cache[stid] = {
                'day': day_key,
                'max_hi': hi_c,
                'min_lo': lo_c,
                'last_update': oso_time.isoformat()
            }
        else:
            # Update accumulated max/min
            old_max = cache[stid].get('max_hi', hi_c)
            old_min = cache[stid].get('min_lo', lo_c)
            cache[stid]['max_hi'] = max(old_max, hi_c)
            cache[stid]['min_lo'] = min(old_min, lo_c)
            cache[stid]['last_update'] = oso_time.isoformat()
        
        # Save updated cache
        _save_oso_cache(cache_file, cache)
        
        # Return accumulated daily max and min in Celsius
        return (cache[stid]['max_hi'], cache[stid]['min_lo'])
        
    except (FileNotFoundError, ValueError, OSError):
        return None, None

def format_hads(
    station: Dict[str, Any], *, day_start: datetime, day_end: datetime, now: datetime
) -> Dict[str, Any]:
    observations = station.get("OBSERVATIONS", {})

    air_temp = observations.get("air_temp_set_1")
    precip_accum = observations.get("precip_accum_set_1")

    date_time = observations.get("date_time") or []
    dt_latest = date_time[-1] if date_time else None

    # Try to get hourly temps from OSO file
    stid = station.get("STID", "")
    import os
    # Use the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    home_dir = os.path.dirname(script_dir)  # Go up one level from src/ to project root
    hourMax, hourMin = _parse_oso_file(stid, now, home_dir)

    daily_maxT, daily_minT = _compute_daily_temp_range(
        air_temp,
        date_time,
        hourMax=hourMax,
        hourMin=hourMin,
        day_start=day_start,
        day_end=day_end,
    )

    maxF = c_to_f(daily_maxT)
    minF = c_to_f(daily_minT)
    currentF = c_to_f(air_temp[-1]) if air_temp else None

    daily_accum = _compute_daily_from_cumulative(
        precip_accum,
        date_time,
        day_start,
        day_end,
    )

    wy_accum = unwrap_cumulative(precip_accum or [])
    wy_latest = wy_accum[-1] if wy_accum else None
    wy_in_station = mm_to_in(wy_latest)
    daily_in = mm_to_in(daily_accum)

    stid = station.get("STID", {})
    wy_in, norm_in, pct = _get_precip_from_acis(stid, now=now)

    if wy_in_station is not None:
        wy_in = wy_in_station
        
        if station.get("STID") == 'SFOC1':
            wy_in = wy_in - 0.12
        
        if norm_in != 9999:
            pct = int((wy_in / norm_in) * 100)

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


def format_asos(
    station_a: Dict[str, Any],
    station_b: Optional[Dict[str, Any]],
    *,
    day_start: datetime,
    day_end: datetime,
    now: datetime,
) -> Dict[str, Any]:
    observations = station_a.get("OBSERVATIONS", {})

    air_temp = observations.get("air_temp_set_1")
    maxT_6hr = observations.get("air_temp_high_6_hour_set_1")
    minT_6hr = observations.get("air_temp_low_6_hour_set_1")

    date_time = observations.get("date_time") or []
    dt_latest = date_time[-1] if date_time else None

    daily_maxT, daily_minT = _compute_daily_temp_range(
        air_temp,
        date_time,
        maxT_6hr,
        minT_6hr,
        day_start=day_start,
        day_end=day_end,
    )

    maxF = c_to_f(daily_maxT)
    minF = c_to_f(daily_minT)
    currentF = c_to_f(air_temp[-1]) if air_temp else None

    precip_section = (station_b or {}).get("OBSERVATIONS", {}).get("precipitation", [])
    hourly = [entry.get("total") for entry in precip_section]
    date_time_b = [entry.get("last_report") for entry in precip_section]

    daily_accum = _compute_precip_from_hourly(
        hourly,
        date_time_b,
        day_start=day_start,
        day_end=day_end,
    )

    daily_in = mm_to_in(daily_accum)

    stid = (station_b or {}).get("STID", [])
    wy_in, norm_in, pct = _get_precip_from_acis(stid, now=now)

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
    type: str,
    day_start: datetime,
    day_end: datetime,
    now: datetime | None = None,
) -> List[Dict[str, Any]]:
    if type is None:
        raise ValueError("Type is required.")

    current = now or datetime.now(timezone.utc)

    if type == "HADS":
        return [
            format_hads(station, day_start=day_start, day_end=day_end, now=current)
            for station in stations_a
        ]

    if type == "ASOS":
        if stations_b is None:
            raise ValueError("stations_b is required when type='ASOS'.")

        if len(stations_a) != len(stations_b):
            raise ValueError(
                f"stations_a and stations_b must be same length for ASOS "
                f"(got {len(stations_a)} and {len(stations_b)})"
            )

        return [
            format_asos(
                station_a,
                station_b,
                day_start=day_start,
                day_end=day_end,
                now=current,
            )
            for station_a, station_b in zip(stations_a, stations_b)
        ]

    raise ValueError(f"Unknown type: {type!r}")
