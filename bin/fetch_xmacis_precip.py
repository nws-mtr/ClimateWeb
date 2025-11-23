#!/usr/bin/env python3
"""Fetch accumulated and normal precipitation from the XMACIS API."""
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Any, Dict

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.xmacis_client import (
    XMACISAPIError,
    XMACISClient,
    start_of_water_year_iso,
)

def load_station_ids(path: str = "config/stations.yaml") -> Dict[str, List[str]]:
    p = Path(path)
    with p.open("r") as f:
        data = yaml.safe_load(f) or {}
    stations = data.get("stations", {})
    # Ensure we always get lists of strings
    return {k: list(v or []) for k, v in stations.items()}

stations = load_station_ids()

ASOS: List[str] = stations.get("ASOS", [])
HADS: List[str] = stations.get("HADS", [])

def load_xmacis_fallbacks(path: str = "config/stations.yaml") -> Dict[str, str]:
    """Load optional XMACIS fallback station IDs from config."""

    p = Path(path)
    with p.open("r") as f:
        data = yaml.safe_load(f) or {}

    raw_fallbacks = data.get("xmacis_fallbacks", {}) or {}

    fallbacks: Dict[str, str] = {}
    for primary, secondary in raw_fallbacks.items():
        if primary is None or secondary is None:
            continue

        primary_id = str(primary).strip()
        secondary_id = str(secondary).strip()

        if not primary_id or not secondary_id:
            continue

        fallbacks[primary_id] = secondary_id

    return fallbacks


XMACIS_FALLBACKS: Dict[str, str] = load_xmacis_fallbacks()


def fetch_xmacis_precip(station: str) -> Dict[str, Any]:
    start = start_of_water_year_iso()
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    client = XMACISClient()

    def _request(station_id: str) -> Dict[str, Any]:
        return client.fetch_precip_with_normals(station_id, start=start, end=end)

    try:
        response = _request(station)
    except XMACISAPIError as exc:
        fallback_station = XMACIS_FALLBACKS.get(station)

        if not fallback_station:
            raise SystemExit(f"Failed to fetch precipitation data: {exc}")

        try:
            response = _request(fallback_station)
            print(
                "Using XMACIS fallback station",
                f"{fallback_station!r} for primary station {station!r}",
            )
        except XMACISAPIError as fallback_exc:
            raise SystemExit(
                "Failed to fetch precipitation data after trying fallback: "
                f"primary={station!r} ({exc}); "
                f"fallback={fallback_station!r} ({fallback_exc})"
            )

    return response.get("smry", [])
