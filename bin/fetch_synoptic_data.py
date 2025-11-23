#!/usr/bin/env python3
import json
import yaml
import sys
from pathlib import Path
from typing import List, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.synoptic_client import SynopticClient, SynopticAPIError
from src.data_processor import build_station_payload

def load_station_ids(config_path: str = "config/stations.yaml") -> dict:
    path = Path(config_path)
    with path.open("r") as f:
        data = yaml.safe_load(f)
    return data.get("stations", {})

stations = load_station_ids()

ASOS: List[str] = stations.get("ASOS", [])
HADS: List[str] = stations.get("HADS", [])

def fetch_synoptic_data() -> Tuple[List[dict], List[dict], List[dict]]:
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

    return stationsA, stationsB, stationsC


def main() -> None:
    stationsA, stationsB, stationsC = fetch_synoptic_data()

    payloadA = build_station_payload(stationsA, stationsB, type="ASOS")
    payloadB = build_station_payload(stationsC, type="HADS")

    combined = payloadA + payloadB

    print(
        "Fetched synoptic data for "
        f"{len(stationsA)} ASOS and {len(stationsC)} HADS stations."
    )
    print(f"Built payloads for {len(combined)} stations.")


if __name__ == "__main__":
    main()
