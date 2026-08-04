"""Fixture-based test of the real Finland PRH/YTJ parsing/pagination logic (no network).

Fixtures below are trimmed-down real shapes observed from a live call to
https://avoindata.prh.fi/opendata-ytj-api/v3/companies?maxResults=100&page=1 on
2026-08-04 (fields not needed for parsing logic have been dropped for brevity, but
the field names/nesting/language-code convention are exactly what the live API
returns).
"""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

PAGE_1 = {
    "totalResults": 822379,
    "companies": [
        {
            "businessId": {"value": "2283132-3", "registrationDate": "2009-08-27"},
            "names": [
                {"name": "Compater Oy", "type": "1", "registrationDate": "2009-09-14"}
            ],
            "addresses": [
                {
                    "type": 2,
                    "street": "Hyljetie",
                    "postCode": "02260",
                    "postOffices": [
                        {"city": "ESBO", "languageCode": "2"},
                        {"city": "ESPOO", "languageCode": "1"},
                    ],
                    "buildingNumber": "18",
                    "entrance": "A",
                }
            ],
            "registeredEntries": [
                {
                    "type": "0",
                    "register": "1",
                    "registrationDate": "2009-08-27",
                    "endDate": "2009-09-13",
                    "descriptions": [
                        {"languageCode": "3", "description": "Unregistered"},
                        {"languageCode": "1", "description": "Rekisteroimaton"},
                    ],
                },
                {
                    "type": "1",
                    "register": "1",
                    "registrationDate": "2009-09-14",
                    "descriptions": [
                        {"languageCode": "3", "description": "Registered"},
                        {"languageCode": "1", "description": "Rekisterissa"},
                    ],
                },
            ],
        },
        {
            "businessId": {"value": "0100002-9", "registrationDate": "1978-03-15"},
            "names": [
                {"name": "Artjarven Metalli Oy", "type": "1", "registrationDate": "1976-04-23", "endDate": "2005-12-19"}
            ],
            "addresses": [],
            "registeredEntries": [
                {
                    "type": "1",
                    "register": "1",
                    "registrationDate": "1976-04-23",
                    "endDate": "2005-12-18",
                    "descriptions": [{"languageCode": "3", "description": "Registered"}],
                },
                {
                    "type": "4",
                    "register": "1",
                    "registrationDate": "2005-12-19",
                    "descriptions": [{"languageCode": "3", "description": "Ceased"}],
                },
            ],
        },
    ],
}
PAGE_2_EMPTY = {"totalResults": 822379, "companies": []}


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_finland_pagination_and_field_parsing(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_finland" in sys.modules:
        del sys.modules["update_finland"]
    mod = importlib.import_module("update_finland")

    pages = [PAGE_1, PAGE_2_EMPTY]

    def _fake_get(url, **kwargs):
        return _FakeResponse(pages.pop(0))

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

    out = tmp_path / "finland_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 2

    active = df.filter(pl.col("registration_number") == "2283132-3").row(0, named=True)
    assert active["company_name"] == "Compater Oy"
    assert active["status"] == "Registered"
    assert active["registered_address"] == "Hyljetie 18 A, 02260, ESPOO"
    assert active["jurisdiction"] == "FI"
    assert active["ubo_names"] is None  # PRH's API has no UBO data — never fabricate one

    ceased = df.filter(pl.col("registration_number") == "0100002-9").row(0, named=True)
    assert ceased["company_name"] == "Artjarven Metalli Oy"
    assert ceased["status"] == "Ceased"
    assert ceased["registered_address"] is None  # no addresses in the source for this company


def test_finland_exits_nonzero_on_request_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_finland" in sys.modules:
        del sys.modules["update_finland"]
    mod = importlib.import_module("update_finland")

    def _boom(url, **kwargs):
        raise mod.requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "finland_reg.parquet").exists()
