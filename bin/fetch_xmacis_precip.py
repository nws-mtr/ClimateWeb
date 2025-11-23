#!/usr/bin/env python3
"""Fetch accumulated and normal precipitation from the XMACIS API."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.xmacis_client import (
    XMACISAPIError,
    XMACISClient,
    start_of_water_year_iso,
)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("Usage: fetch_xmacis_precip.py <STATION_ID>")

    station = sys.argv[1]
    start = start_of_water_year_iso()
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    client = XMACISClient()
    try:
        response = client.fetch_precip_with_normals(station, start=start, end=end)
    except XMACISAPIError as exc:
        raise SystemExit(f"Failed to fetch precipitation data: {exc}")

    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
