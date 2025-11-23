"""Client for interacting with the XMACIS API."""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from json import JSONDecodeError

class XMACISAPIError(Exception):
    """Custom error for XMACIS API issues."""


class XMACISClient:
    """Simple client for fetching precipitation data from XMACIS."""

    BASE_URL = "https://data.rcc-acis.org/StnData"

    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout

    def fetch_precip_with_normals(
        self,
        station: str,
        *,
        start: str,
        end: str,
        ) -> Dict[str, Any]:
        """Fetch accumulated and normal precipitation from XMACIS."""
        if not isinstance(station, str) or not station.strip():
            raise XMACISAPIError(f"Invalid station passed to XMACIS: {station!r}")
        
        payload = {
            "sid": station,
            "sdate": start,
            "edate": end,
            "elems": [
                {
                    "name": "pcpn",
                    "interval": "dly",
                    "duration": "dly",
                    "smry": {"reduce": "sum"},
                    "smry_only": 1,
                },
                {
                    "name": "pcpn",
                    "interval": "dly",
                    "duration": "dly",
                    "smry": {"reduce": "sum"},
                    "normal": 1,
                    "smry_only": 1,
                },
            ],
        }

        data = urllib.parse.urlencode({"params": json.dumps(payload)})
        req = urllib.request.Request(
            self.BASE_URL,
            data=data.encode("utf-8"),
            headers={"Accept": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                body = response.read().decode("utf-8", errors="replace")

                if response.status != 200:
                    raise XMACISAPIError(
                        f"Request failed with status {response.status} for station={station}, "
                        f"sdate={start}, edate={end}. Body snippet: {body[:300]!r}"
                    )

                if not body.strip():
                    raise XMACISAPIError(
                        f"Empty response body from XMACIS for station={station}, "
                        f"sdate={start}, edate={end}"
                    )

                try:
                    parsed = json.loads(body)
                except JSONDecodeError as e:
                    raise XMACISAPIError(
                        f"Non-JSON response from XMACIS for station={station}, "
                        f"sdate={start}, edate={end}. Body snippet: {body[:300]!r}"
                    ) from e

        except HTTPError as exc:
            body = exc.read()
            details = body.decode("utf-8", errors="ignore") if body else exc.reason
            raise XMACISAPIError(
                f"HTTP error {exc.code} during API call for station={station}: "
                f"{details or 'no response body'}"
            )
        except URLError as exc:
            raise XMACISAPIError(f"Network error during API call for station={station}: {exc}")

        if "error" in parsed:
            raise XMACISAPIError(f"API error for station={station}: {parsed['error']}")

        return parsed


def start_of_water_year_iso(now: datetime | None = None) -> str:
    """Return the ACIS-friendly start date derived from ``start_date``.

    The :func:`lib.synoptic_client.start_date` function returns ``YYYYMMDDHHMM``.
    XMACIS expects dates in ``YYYY-MM-DD``; this helper bridges the formats.
    """

    if now is None:
        now = datetime.now(timezone.utc)

    from lib.synoptic_client import start_date

    wateryear_start = start_date(now)
    dt = datetime.strptime(wateryear_start, "%Y%m%d%H%M")
    return dt.strftime("%Y-%m-%d")
