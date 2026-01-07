#!/usr/bin/env python3
"""Build payloads from Synoptic and XMACIS responses."""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bin.fetch_synoptic_data import fetch_synoptic_data
from src.data_processor import build_station_payload, climate_day_window

OUTPUT_PATH = ROOT_DIR / "web" / "station_payloads.json"
YESTERDAY_OUTPUT_PATH = ROOT_DIR / "web" / "station_payloads_yesterday.json"

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


def build_payloads(current_time: datetime | None = None) -> tuple[Dict[str, Any], Dict[str, Any]]:
    now = current_time or datetime.now(timezone.utc)

    stationsA, stationsB, stationsC = fetch_synoptic_data(current_time=now)

    today_start, today_end = climate_day_window(now, days_ago=0)
    yesterday_start, yesterday_end = climate_day_window(now, days_ago=1)

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
    )

    combined_today: List[Dict[str, Any]] = payloadA_today + payloadB_today
    combined_yesterday: List[Dict[str, Any]] = payloadA_yesterday + payloadB_yesterday

    today_payload = {
        "meta": {
            "generatedAt": now.isoformat(),
            "climateDayStart": today_start.isoformat(),
            "climateDayEnd": today_end.isoformat(),
            "climateDayLabel": _format_day_label(today_start),
        },
        "data": combined_today,
    }

    yesterday_payload = {
        "meta": {
            "generatedAt": now.isoformat(),
            "climateDayStart": yesterday_start.isoformat(),
            "climateDayEnd": yesterday_end.isoformat(),
            "climateDayLabel": _format_day_label(yesterday_start),
        },
        "data": combined_yesterday,
    }

    return today_payload, yesterday_payload


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
    args = parser.parse_args()

    override_time = _parse_as_of(args.as_of)
    now = override_time or datetime.now(timezone.utc)

    today_payload, yesterday_payload = build_payloads(now)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(today_payload, indent=2), encoding="utf-8")
    YESTERDAY_OUTPUT_PATH.write_text(json.dumps(yesterday_payload, indent=2), encoding="utf-8")

    print(
        f"Saved station payload and precipitation summary to {OUTPUT_PATH} "
        f"and yesterday snapshot to {YESTERDAY_OUTPUT_PATH}"
    )


if __name__ == "__main__":
    main()
