#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.synoptic_client import SynopticClient, SynopticAPIError
from src.data_processor import build_station_payload

STATION_IDS = [
    "OAMC1",
    "KCCR",
    "SARC1",
    "KSNS",
    "SFOC1",
    "KSFO",
    "HMBC1",
    "RWCC1",
    "KSJC",
    "PKFC1",
    "SRTC1",
    "HDZC1",
    "CTOC1",
]

OUTPUT_PATH = Path("data/synoptic_stations.json")


def main() -> None:
    client = SynopticClient()
    try:
        response = client.fetch_latest(STATION_IDS)
    except SynopticAPIError as exc:
        raise SystemExit(f"Failed to fetch station data: {exc}")

    stations = response.get("STATION", [])
    payload = build_station_payload(stations)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Saved data for {len(payload)} stations to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
