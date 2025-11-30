#!/usr/bin/env python3
"""Build payloads from Synoptic and XMACIS responses."""
import json
import sys
from pathlib import Path
from typing import List, Dict, Any

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bin.fetch_synoptic_data import fetch_synoptic_data
from src.data_processor import build_station_payload

OUTPUT_PATH = Path("web/station_payloads.json")

def build_payloads() -> Dict[str, Any]:
    stationsA, stationsB, stationsC = fetch_synoptic_data()

    payloadA = build_station_payload(stationsA, stationsB, type="ASOS")
    payloadB = build_station_payload(stationsC, type="HADS")

    combined: List[Dict[str, Any]] = payloadA + payloadB

    return {
        "data": combined,
    }

def main() -> None:
    payloads = build_payloads()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payloads, indent=2), encoding="utf-8")

    print(
        f"Saved station payload and precipitation summary "
        f"to {OUTPUT_PATH}"
    )

if __name__ == "__main__":
    main()
