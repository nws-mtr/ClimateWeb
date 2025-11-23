#!/usr/bin/env python3
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lib.synoptic_client import SynopticClient, SynopticAPIError

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

def fetch_synoptic_data() -> tuple[list[dict], list[dict], list[dict]]:
    """Fetch raw station responses from the Synoptic API."""

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

    payload = {
        "stationsA": stationsA,
        "stationsB": stationsB,
        "stationsC": stationsC,
    }

    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
