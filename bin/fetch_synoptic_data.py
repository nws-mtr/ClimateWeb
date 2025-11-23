#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.synoptic_client import SynopticClient, SynopticAPIError
from src.data_processor import build_station_payload

ASOS = [
    "KCCR",
    "KSNS",
    "KSFO",
    "KSJC",
    "KMRY",
]

HADS = [
    "OAMC1",
    "SARC1",
    "SFOC1",
    "RWCC1",
    "PKFC1",
    "SRTC1",
    "HDZC1",
    "CTOC1",
]

OUTPUT_PATH = Path("synoptic_stations.json")


def main() -> None:
    client = SynopticClient()
    try:
        responseA = client.fetch_latest(ASOS)
        responseB = client.fetch_precip(ASOS)
        responseC = client.fetch_timeseries(HADS)
    except SynopticAPIError as exc:
        raise SystemExit(f"Failed to fetch station data: {exc}")

    stationsA = responseA.get("STATION", [])
    stationsB = responseB.get("STATION", [])
    stationsC = responseC.get("STATION", [])
    payloadA = build_station_payload(stationsA, stationsB, type="ASOS")
    payloadB = build_station_payload(stationsC, type="HADS")

    combined = payloadA + payloadB

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(combined, indent=2), encoding="utf-8")
    print(f"Saved data for {len(combined)} stations to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
