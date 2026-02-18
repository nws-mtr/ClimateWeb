#!/usr/bin/env python3
"""Build payloads from Synoptic and XMACIS responses."""
import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bin.fetch_synoptic_data import fetch_synoptic_data
from src.data_processor import build_station_payload, climate_day_window

# Global flag to determine which paths to use
USE_DEV_PATHS = True  # Set to False for production

if USE_DEV_PATHS:
    OUTPUT_PATH = ROOT_DIR / "web" / "station_payloads.json"
    YESTERDAY_OUTPUT_PATH = ROOT_DIR / "web" / "station_payloads_yesterday.json"
    YESTERDAY_MARKER_PATH = ROOT_DIR / "web" / ".yesterday_generated"
    OSO_CACHE_PATH = ROOT_DIR / "oso_cache.json"
    OSO_CACHE_YESTERDAY_PATH = ROOT_DIR / "oso_cache_yesterday.json"
else:
    OUTPUT_PATH = Path("/ldad/localapps/climateWeb/web/station_payloads.json")
    YESTERDAY_OUTPUT_PATH = Path("/ldad/localapps/climateWeb/web/station_payloads_yesterday.json")
    YESTERDAY_MARKER_PATH = Path("/ldad/localapps/climateWeb/web/.yesterday_generated")
    OSO_CACHE_PATH = Path("/ldad/localapps/climateWeb/db/oso_cache.json")
    OSO_CACHE_YESTERDAY_PATH = Path("/ldad/localapps/climateWeb/db/oso_cache_yesterday.json")
    RSYNC_PATH = Path("/data/ldad/CmsRsyncManager/data/incoming/PublicData/climateWeb/station_payloads.json")
    YESTERDAY_RSYNC_PATH = Path("/data/ldad/CmsRsyncManager/data/incoming/PublicData/climateWeb/station_payloads_yesterday.json")


def _format_day_label(day_start: datetime) -> str:
    try:
        from zoneinfo import ZoneInfo
    except ImportError:  # pragma: no cover
        from backports.zoneinfo import ZoneInfo  # type: ignore

    try:
        pacific = ZoneInfo("America/Los_Angeles")
        return day_start.astimezone(pacific).strftime("%Y-%m-%d")
    except Exception:
        return day_start.strftime("%Y-%m-%d")


def _should_archive(current_day_label: str, now: datetime) -> bool:
    """Check if we need to archive at 0900 on the new climate day."""
    if not OUTPUT_PATH.exists():
        return False

    day_start, _ = climate_day_window(now, days_ago=0)
    archive_time = day_start + timedelta(hours=1)
    if now < archive_time:
        return False
    
    try:
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            existing = json.load(f)
        
        existing_label = existing.get('meta', {}).get('climateDayLabel')
        if not existing_label:
            return False
        return existing_label != current_day_label
    except (json.JSONDecodeError, IOError):
        return False


def _archive_oso_cache() -> None:
    if not OSO_CACHE_PATH.exists():
        return

    OSO_CACHE_YESTERDAY_PATH.parent.mkdir(parents=True, exist_ok=True)
    OSO_CACHE_YESTERDAY_PATH.write_text(OSO_CACHE_PATH.read_text(encoding="utf-8"), encoding="utf-8")


def _parse_as_of(as_of: str | None) -> datetime | None:
    if not as_of:
        return None

    text = as_of.strip()
    if not text:
        return None

    cleaned = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:  # pragma: no cover - handled via CLI feedback
        raise SystemExit(f"Invalid --as-of value {text!r}: {exc}")

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed


def build_payloads(current_time: datetime | None = None) -> Dict[str, Any]:
    now = current_time or datetime.now(timezone.utc)

    stationsA, stationsB, stationsC = fetch_synoptic_data(current_time=now)

    today_start, today_end = climate_day_window(now, days_ago=0)

    payloadA_today = build_station_payload(
        stationsA,
        stationsB,
        type="ASOS",
        day_start=today_start,
        day_end=today_end,
        now=now,
        is_current_day=True,
    )
    payloadB_today = build_station_payload(
        stationsC,
        type="HADS",
        day_start=today_start,
        day_end=today_end,
        now=now,
        is_current_day=True,
    )

    combined_today: List[Dict[str, Any]] = payloadA_today + payloadB_today

    today_payload = {
        "meta": {
            "generatedAt": now.isoformat(),
            "climateDayStart": today_start.isoformat(),
            "climateDayEnd": today_end.isoformat(),
            "climateDayLabel": _format_day_label(today_start),
        },
        "data": combined_today,
    }

    return today_payload


def should_generate_yesterday(now: datetime) -> bool:
    """
    Check if yesterday's payload should be regenerated based on climate day.
    
    Returns True if:
    - No marker file exists (first run)
    - Marker file is invalid/corrupted
    - We've crossed into a new climate day since last generation
    """
    if not YESTERDAY_MARKER_PATH.exists():
        return True
    
    try:
        last_run = datetime.fromisoformat(YESTERDAY_MARKER_PATH.read_text().strip())
        # Get the climate day start for both timestamps
        last_day_start, _ = climate_day_window(last_run, days_ago=0)
        current_day_start, _ = climate_day_window(now, days_ago=0)
        
        # Regenerate if we've crossed into a new climate day
        return current_day_start > last_day_start
    except (ValueError, OSError):
        return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Build station payloads for current and previous climate days.")
    parser.add_argument(
        "--as-of",
        dest="as_of",
        help=(
            "Override the current UTC time (ISO 8601) for testing, "
            "e.g., 2025-12-01T08:00:00Z"
        ),
    )
    parser.add_argument(
        "--force-yesterday",
        action="store_true",
        help="Force regeneration of yesterday's payload even if already generated today"
    )
    args = parser.parse_args()

    override_time = _parse_as_of(args.as_of)
    now = override_time or datetime.now(timezone.utc)

    today_payload = build_payloads(now)
    current_day_label = today_payload['meta']['climateDayLabel']

    # Check if we need to generate yesterday's payload (on day transition)
    if _should_archive(current_day_label, now):
        try:
            # Generate yesterday's payload explicitly using yesterday's OSO cache
            print(f"New climate day detected, generating yesterday's payload...")
            
            _archive_oso_cache()
            stationsA, stationsB, stationsC = fetch_synoptic_data(current_time=now)
            yesterday_start, yesterday_end = climate_day_window(now, days_ago=1)
            
            payloadA_yesterday = build_station_payload(
                stationsA,
                stationsB,
                type="ASOS",
                day_start=yesterday_start,
                day_end=yesterday_end,
                now=now,
                is_current_day=False,
            )
            payloadB_yesterday = build_station_payload(
                stationsC,
                type="HADS",
                day_start=yesterday_start,
                day_end=yesterday_end,
                now=now,
                is_current_day=False,
                oso_cache_file=str(OSO_CACHE_YESTERDAY_PATH),
            )
            
            combined_yesterday = payloadA_yesterday + payloadB_yesterday
            
            yesterday_payload = {
                "meta": {
                    "generatedAt": now.isoformat(),
                    "climateDayStart": yesterday_start.isoformat(),
                    "climateDayEnd": yesterday_end.isoformat(),
                    "climateDayLabel": _format_day_label(yesterday_start),
                },
                "data": combined_yesterday,
            }
            
            # Write yesterday's payload
            YESTERDAY_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
            YESTERDAY_OUTPUT_PATH.write_text(json.dumps(yesterday_payload, indent=2), encoding="utf-8")
            print(f"Generated yesterday's payload: {YESTERDAY_OUTPUT_PATH}")
            
            if not USE_DEV_PATHS:
                YESTERDAY_RSYNC_PATH.parent.mkdir(parents=True, exist_ok=True)
                YESTERDAY_RSYNC_PATH.write_text(json.dumps(yesterday_payload, indent=2), encoding="utf-8")
                
        except Exception as e:
            print(f"Warning: Could not generate yesterday's payload: {e}")

    # Write today's payload
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(today_payload, indent=2), encoding="utf-8")

    if not USE_DEV_PATHS:
        RSYNC_PATH.parent.mkdir(parents=True, exist_ok=True)
        RSYNC_PATH.write_text(json.dumps(today_payload, indent=2), encoding="utf-8")

    print(f"Saved station payload to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
