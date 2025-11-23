#!/usr/bin/env python3
"""Fetch accumulated and normal precipitation from the XMACIS API."""

import json
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.xmacis_client import (
    XMACISAPIError,
    XMACISClient,
    start_of_water_year_iso,
)

def load_station_ids(config_path: str = "config/stations.yaml") -> dict:
    path = Path(config_path)
    with path.open("r") as f:
        data = yaml.safe_load(f)
    return data.get("stations", {})

stations = load_station_ids()

ASOS: List[str] = stations.get("ASOS", [])
HADS: List[str] = stations.get("HADS", [])

def main() -> None:

    start = start_of_water_year_iso()
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    client = XMACISClient()
    try:
        response = client.fetch_precip_with_normals(ASOS[0], start=start, end=end)
    except XMACISAPIError as exc:
        raise SystemExit(f"Failed to fetch precipitation data: {exc}")

    vals = response.get("smry", [])

if __name__ == "__main__":
    main()
