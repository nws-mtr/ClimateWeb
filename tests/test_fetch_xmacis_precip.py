from datetime import datetime, timezone
from pathlib import Path

import pytest

from bin import fetch_xmacis_precip as module
from lib.xmacis_client import XMACISAPIError


def test_load_station_ids_returns_lists(tmp_path):
    config = tmp_path / "stations.yaml"
    config.write_text(
        """stations:
  ASOS: [A1, A2]
  HADS:
    - H1
    - H2
        """
    )

    stations = module.load_station_ids(str(config))
    assert stations == {"ASOS": ["A1", "A2"], "HADS": ["H1", "H2"]}


def test_load_xmacis_fallbacks_filters_invalid_entries(tmp_path):
    config = tmp_path / "stations.yaml"
    config.write_text(
        """xmacis_fallbacks:
  GOOD: ALT
  "": IGNORED
  bad: ""
        """
    )

    fallbacks = module.load_xmacis_fallbacks(str(config))
    assert fallbacks == {"GOOD": "ALT"}


def test_fetch_xmacis_precip_uses_fallback(monkeypatch):
    calls = []

    def fake_fetch(self, station, *, start, end):
        calls.append((station, start, end))
        if station == "PRIMARY":
            raise XMACISAPIError("no data available")
        return {"smry": ["ok"]}

    monkeypatch.setattr(module.XMACISClient, "fetch_precip_with_normals", fake_fetch)
    module.XMACIS_FALLBACKS = {"PRIMARY": "FALLBACK"}

    response = module.fetch_xmacis_precip("PRIMARY")

    assert response == ["ok"]
    assert calls[0][0] == "PRIMARY"
    assert calls[1][0] == "FALLBACK"
    assert calls[0][1] <= calls[0][2]  # start date should not exceed end date


def test_fetch_xmacis_precip_raises_after_failed_fallback(monkeypatch):
    def always_fail(self, station, *, start, end):
        raise XMACISAPIError(f"no data for {station}")

    monkeypatch.setattr(module.XMACISClient, "fetch_precip_with_normals", always_fail)
    module.XMACIS_FALLBACKS = {"PRIMARY": "FALLBACK"}

    with pytest.raises(SystemExit) as excinfo:
        module.fetch_xmacis_precip("PRIMARY")

    msg = str(excinfo.value)
    assert "primary='PRIMARY'" in msg
    assert "fallback='FALLBACK'" in msg
