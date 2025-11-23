import pytest

from bin import fetch_xmacis_precip as module
from lib.xmacis_client import XMACISAPIError


def test_fetch_xmacis_precip_uses_fallback(monkeypatch):
    calls = []

    def fake_fetch(self, station, *, start, end):  # pragma: no cover - behavior verified via assertions
        calls.append(station)
        if station == "PRIMARY":
            raise XMACISAPIError("no data available")
        return {"smry": ["ok"]}

    monkeypatch.setattr(module.XMACISClient, "fetch_precip_with_normals", fake_fetch)
    module.XMACIS_FALLBACKS = {"PRIMARY": "FALLBACK"}

    response = module.fetch_xmacis_precip("PRIMARY")

    assert response == ["ok"]
    assert calls == ["PRIMARY", "FALLBACK"]


def test_fetch_xmacis_precip_raises_after_failed_fallback(monkeypatch):
    def always_fail(self, station, *, start, end):  # pragma: no cover - behavior verified via exception
        raise XMACISAPIError(f"no data for {station}")

    monkeypatch.setattr(module.XMACISClient, "fetch_precip_with_normals", always_fail)
    module.XMACIS_FALLBACKS = {"PRIMARY": "FALLBACK"}

    with pytest.raises(SystemExit) as excinfo:
        module.fetch_xmacis_precip("PRIMARY")

    msg = str(excinfo.value)
    assert "primary='PRIMARY'" in msg
    assert "fallback='FALLBACK'" in msg
