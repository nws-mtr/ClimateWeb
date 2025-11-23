#!/usr/bin/env python3
"""Combine Synoptic station payloads with XMACIS precipitation summaries."""

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bin.fetch_synoptic_data import fetch_synoptic_data
from bin.fetch_xmacis_precip import fetch_xmacis_precip
from src.data_processor import build_station_payload

DEFAULT_OUTPUT = Path("synoptic_stations.json")


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(
            "Usage: build_station_payloads.py <XMACIS_STATION> [OUTPUT_PATH]"
        )

    xmacis_station = sys.argv[1]
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_OUTPUT

    stationsA, stationsB, stationsC = fetch_synoptic_data()

    payload_asos = build_station_payload(stationsA, stationsB, type="ASOS")
    payload_hads = build_station_payload(stationsC, type="HADS")
    combined = payload_asos + payload_hads

    precip_summary = fetch_xmacis_precip(xmacis_station)

    result = {
        "stations": combined,
        "xmacisSummary": precip_summary,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Saved station payload and XMACIS summary to {output_path}")


if __name__ == "__main__":
    main()
