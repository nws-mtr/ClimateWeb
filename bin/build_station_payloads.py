#!/usr/bin/env python3
"""Build payloads from Synoptic and XMACIS responses."""

import json
from pathlib import Path
from typing import List, Dict, Any

from bin.fetch_synoptic_data import fetch_synoptic_data
from bin.fetch_xmacis_precip import fetch_xmacis_precip
from src.data_processor import build_station_payload

OUTPUT_PATH = Path("station_payloads.json")


def build_payloads() -> Dict[str, Any]:
    stationsA, stationsB, stationsC = fetch_synoptic_data()

    payloadA = build_station_payload(stationsA, stationsB, type="ASOS")
    payloadB = build_station_payload(stationsC, type="HADS")

    precip_summary = fetch_xmacis_precip()

    combined: List[Dict[str, Any]] = payloadA + payloadB

    return {
        "stations": combined,
        "precipSummary": precip_summary,
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
