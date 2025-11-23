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

def fetch_xmacis_precip(station: Dict[str, Any]) -> Dict[str, Any]:
    start = start_of_water_year_iso()
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    client = XMACISClient()
    try:
        response = client.fetch_precip_with_normals(station, start=start, end=end)
    except XMACISAPIError as exc:
        raise SystemExit(f"Failed to fetch precipitation data: {exc}")

    return response.get("smry", [])
