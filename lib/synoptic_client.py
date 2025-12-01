import json
import os
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
from typing import Any, Dict, List
from datetime import datetime, timezone


def start_date(now: datetime | None = None) -> str:
    """
    Return the most recent past (or equal) Oct 1 0700Z
    as a string YYYYMMDDHHMM.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    year = now.year
    target = datetime(year, 10, 1, 7, 0, tzinfo=timezone.utc)

    if now < target:
        year -= 1
        target = datetime(year, 10, 1, 7, 0, tzinfo=timezone.utc)

    return target.strftime("%Y%m%d%H%M")


class SynopticAPIError(Exception):
    """Custom error for Synoptic API issues."""


class SynopticClient:
    """Client for interacting with the Synoptic API."""

    TS_URL = "https://api.synopticdata.com/v2/stations/timeseries"
    LATEST_URL = "https://api.synopticdata.com/v2/stations/timeseries"
    PRECIP_URL = "https://api.synopticdata.com/v2/stations/precipitation"

    def __init__(self, api_key: str | None = None, now: datetime | None = None) -> None:
        # Strip whitespace to avoid accidental quote/newline issues from env files.
        self.api_key = (api_key or os.environ.get("SYNOPTIC_KEY", "")).strip()
        if not self.api_key:
            raise SynopticAPIError(
                "Synoptic API key is missing. Set the SYNOPTIC_KEY environment variable."
            )
        # Allow the caller to control "now" (useful for snapshots/tests).
        self.now = now or datetime.now(timezone.utc)

    def fetch_timeseries(self, station_ids: List[str]) -> Dict[str, Any]:
        """Fetch the timeseries of observations for the given station IDs.

        Args:
            station_ids: List of station identifiers.

        Returns:
            Parsed JSON response from the API.

        Raises:
            SynopticAPIError: When the API request fails or returns an error.
        """
        params = {
            "stid": ",".join(station_ids),
            "token": self.api_key,
            "vars": "air_temp,precip_accum",
            "showemptystations": 1,
            "start": start_date(self.now),
            "end": self.now.strftime("%Y%m%d%H%M"),
            "hfmetars": 0,
        }
        query = urllib.parse.urlencode(params)
        url = f"{self.TS_URL}?{query}"

        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                if response.status != 200:
                    raise SynopticAPIError(
                        f"Request failed with status {response.status}: {response.read()}"
                    )

                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            # Surface response body for clearer debugging (e.g., invalid token or auth issues).
            body = exc.read()
            details = body.decode("utf-8", errors="ignore") if body else exc.reason
            raise SynopticAPIError(
                f"HTTP error {exc.code} during API call: {details or 'no response body'}"
            )
        except URLError as exc:
            raise SynopticAPIError(f"Network error during API call: {exc}")

        if payload.get("SUMMARY", {}).get("RESPONSE_CODE") != 1:
            raise SynopticAPIError(
                f"API error: {payload.get('SUMMARY', {}).get('RESPONSE_MESSAGE')}"
            )

        return payload

    def fetch_latest(self, station_ids: List[str]) -> Dict[str, Any]:
        """Fetch the latest observations for the given station IDs.

        Args:
            station_ids: List of station identifiers.

        Returns:
            Parsed JSON response from the API.

        Raises:
            SynopticAPIError: When the API request fails or returns an error.
        """
        params = {
            "stid": ",".join(station_ids),
            "token": self.api_key,
            "vars": "air_temp,air_temp_high_6_hour,air_temp_low_6_hour",
            "showemptystations": 1,
            "recent": 1440,
            "hfmetars": 0,
        }
        query = urllib.parse.urlencode(params)
        url = f"{self.LATEST_URL}?{query}"

        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                if response.status != 200:
                    raise SynopticAPIError(
                        f"Request failed with status {response.status}: {response.read()}"
                    )

                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            # Surface response body for clearer debugging (e.g., invalid token or auth issues).
            body = exc.read()
            details = body.decode("utf-8", errors="ignore") if body else exc.reason
            raise SynopticAPIError(
                f"HTTP error {exc.code} during API call: {details or 'no response body'}"
            )
        except URLError as exc:
            raise SynopticAPIError(f"Network error during API call: {exc}")

        if payload.get("SUMMARY", {}).get("RESPONSE_CODE") != 1:
            raise SynopticAPIError(
                f"API error: {payload.get('SUMMARY', {}).get('RESPONSE_MESSAGE')}"
            )

        return payload

    def fetch_precip(self, station_ids: List[str]) -> Dict[str, Any]:
        """Fetch the timeseries of observations for the given station IDs.

        Args:
            station_ids: List of station identifiers.

        Returns:
            Parsed JSON response from the API.

        Raises:
            SynopticAPIError: When the API request fails or returns an error.
        """
        params = {
            "stid": ",".join(station_ids),
            "token": self.api_key,
            "pmode": "intervals",
            "interval": "hour",
            "showemptystations": 1,
            "start": start_date(self.now),
            "end": self.now.strftime("%Y%m%d%H%M"),
        }

        query = urllib.parse.urlencode(params)
        url = f"{self.PRECIP_URL}?{query}"

        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                if response.status != 200:
                    raise SynopticAPIError(
                        f"Request failed with status {response.status}: {response.read()}"
                    )

                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            # Surface response body for clearer debugging (e.g., invalid token or auth issues).
            body = exc.read()
            details = body.decode("utf-8", errors="ignore") if body else exc.reason
            raise SynopticAPIError(
                f"HTTP error {exc.code} during API call: {details or 'no response body'}"
            )
        except URLError as exc:
            raise SynopticAPIError(f"Network error during API call: {exc}")

        if payload.get("SUMMARY", {}).get("RESPONSE_CODE") != 1:
            raise SynopticAPIError(
                f"API error: {payload.get('SUMMARY', {}).get('RESPONSE_MESSAGE')}"
            )

        return payload
