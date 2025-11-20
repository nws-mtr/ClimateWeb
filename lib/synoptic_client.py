import json
import os
import urllib.parse
import urllib.request
from urllib.error import URLError
from typing import Any, Dict, List


class SynopticAPIError(Exception):
    """Custom error for Synoptic API issues."""


class SynopticClient:
    """Client for interacting with the Synoptic API."""

    BASE_URL = "https://api.synopticdata.com/v2/stations/latest"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.environ.get("SYNOPTIC_KEY")
        if not self.api_key:
            raise SynopticAPIError(
                "Synoptic API key is missing. Set the SYNOPTIC_KEY environment variable."
            )

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
            "vars": "air_temp,relative_humidity",
            "showemptystations": 1,
        }
        query = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}?{query}"

        try:
            with urllib.request.urlopen(url, timeout=20) as response:
                if response.status != 200:
                    raise SynopticAPIError(
                        f"Request failed with status {response.status}: {response.read()}"
                    )

                payload = json.loads(response.read().decode("utf-8"))
        except URLError as exc:
            raise SynopticAPIError(f"Network error during API call: {exc}")

        if payload.get("SUMMARY", {}).get("RESPONSE_CODE") != 1:
            raise SynopticAPIError(
                f"API error: {payload.get('SUMMARY', {}).get('RESPONSE_MESSAGE')}"
            )

        return payload
