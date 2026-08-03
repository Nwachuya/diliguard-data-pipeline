"""Fixture-based test of the real Slovakia RPVS parsing/pagination logic (no network)."""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

PAGE_1 = {
    "value": [
        {
            "Id": 1,
            "CisloVlozky": 1,
            "PartneriVerejnehoSektora": [
                {
                    "ObchodneMeno": "TEST FIXTURE s. r. o.",
                    "Ico": "44543832",
                    "PlatnostOd": "2017-02-01T00:00:00+01:00",
                    "PlatnostDo": None,
                }
            ],
            "KonecniUzivateliaVyhod": [
                {"Meno": "Jaroslav", "Priezvisko": "Vidra", "PlatnostOd": "2017-02-01T00:00:00+01:00", "PlatnostDo": None},
                {"Meno": "Katarina", "Priezvisko": "Vidrova", "PlatnostOd": "2017-02-01T00:00:00+01:00", "PlatnostDo": "2020-01-01T00:00:00+01:00"},
            ],
        }
    ],
    "@odata.nextLink": "https://rpvs.gov.sk/opendatav2/Partneri?$skiptoken=Id-1",
}
PAGE_2 = {
    "value": [
        {
            "Id": 2,
            "CisloVlozky": 2,
            "PartneriVerejnehoSektora": [
                {"ObchodneMeno": "TERMINATED FIXTURE a.s.", "Ico": "11111111", "PlatnostOd": "2015-01-01T00:00:00+01:00", "PlatnostDo": "2018-01-01T00:00:00+01:00"}
            ],
            "KonecniUzivateliaVyhod": [],
        }
    ],
    "@odata.nextLink": None,
}


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_slovakia_pagination_and_ubo_join(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_slovakia_rpvs" in sys.modules:
        del sys.modules["update_slovakia_rpvs"]
    mod = importlib.import_module("update_slovakia_rpvs")

    pages = [PAGE_1, PAGE_2]

    def _fake_get(url, **kwargs):
        return _FakeResponse(pages.pop(0))

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

    out = tmp_path / "slovakia_rpvs.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 2

    active = df.filter(pl.col("registration_number") == "44543832").row(0, named=True)
    assert active["company_name"] == "TEST FIXTURE s. r. o."
    assert active["status"] == "ACTIVE"
    assert active["ubo_names"] == "Jaroslav Vidra"  # the lapsed UBO (PlatnostDo set) is excluded

    terminated = df.filter(pl.col("registration_number") == "11111111").row(0, named=True)
    assert terminated["status"] == "TERMINATED"
    assert terminated["ubo_names"] is None


def test_slovakia_exits_nonzero_on_request_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_slovakia_rpvs" in sys.modules:
        del sys.modules["update_slovakia_rpvs"]
    mod = importlib.import_module("update_slovakia_rpvs")

    def _boom(url, **kwargs):
        raise mod.requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "slovakia_rpvs.parquet").exists()
