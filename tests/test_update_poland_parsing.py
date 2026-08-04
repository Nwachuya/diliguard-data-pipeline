"""Fixture-based test of the real Poland KRS parsing/enumeration logic (no network)."""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest
import requests

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Real response shapes observed live from api-krs.ms.gov.pl on 2026-08-04 for
# KRS 0000006865 (CD PROJEKT SPOLKA AKCYJNA, active) and KRS 0000500000 (a
# foundation in liquidation), trimmed to the fields this script reads.
ACTIVE_ENTITY = {
    "odpis": {
        "rodzaj": "Aktualny",
        "naglowekA": {
            "rejestr": "RejP",
            "numerKRS": "0000006865",
        },
        "dane": {
            "dzial1": {
                "danePodmiotu": {
                    "formaPrawna": "SPOLKA AKCYJNA",
                    "identyfikatory": {"regon": "49270733300000", "nip": "7342867148"},
                    "nazwa": "CD PROJEKT SPOLKA AKCYJNA",
                },
                "siedzibaIAdres": {
                    "siedziba": {"kraj": "POLSKA", "wojewodztwo": "MAZOWIECKIE", "miejscowosc": "WARSZAWA"},
                    "adres": {
                        "ulica": "JAGIELLONSKA",
                        "nrDomu": "74",
                        "miejscowosc": "WARSZAWA",
                        "kodPocztowy": "03-301",
                        "poczta": "WARSZAWA",
                        "kraj": "POLSKA",
                    },
                },
            },
            "dzial6": {},
        },
    }
}

LIQUIDATION_ENTITY = {
    "odpis": {
        "rodzaj": "Aktualny",
        "naglowekA": {
            "rejestr": "RejS",
            "numerKRS": "0000500000",
        },
        "dane": {
            "dzial1": {
                "danePodmiotu": {
                    "formaPrawna": "FUNDACJA",
                    "identyfikatory": {"regon": "36000000000000"},
                    "nazwa": "FUNDACJA SWIATOWEGO TYGODNIA PRZEDSIEBIORCZOSCI W LIKWIDACJI",
                },
                "siedzibaIAdres": {
                    "adres": {
                        "ulica": "CHLODNA",
                        "nrDomu": "15",
                        "nrLokalu": "511",
                        "miejscowosc": "WARSZAWA",
                        "kodPocztowy": "00-891",
                        "kraj": "POLSKA",
                    },
                },
            },
            "dzial6": {
                "likwidacja": [{"otwarcieLikwidacji": "OSWIADCZENIE FUNDATORA, 15.01.2026"}],
                "rozwiazanieUniewaznienie": {"okreslenieOkolicznosci": "ROZWIAZANIE"},
            },
        },
    }
}


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class _FakeNotFound(requests.HTTPError):
    def __init__(self):
        response = requests.Response()
        response.status_code = 404
        super().__init__("404 Client Error", response=response)


def test_poland_krs_enumeration_and_status_parsing(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_poland" in sys.modules:
        del sys.modules["update_poland"]
    mod = importlib.import_module("update_poland")

    monkeypatch.setattr(mod, "KRS_RANGE_START", 6865)
    monkeypatch.setattr(mod, "KRS_RANGE_END", 6868)  # scans 6865, 6866, 6867
    monkeypatch.setattr(mod, "MISS_SLEEP_SECONDS", 0)

    def _fake_get(url, **kwargs):
        if url.endswith("/0000006865"):
            return _FakeResponse(ACTIVE_ENTITY)
        if url.endswith("/0000006866"):
            raise _FakeNotFound()  # unassigned KRS number
        if url.endswith("/0000006867"):
            return _FakeResponse(LIQUIDATION_ENTITY)
        raise AssertionError(f"unexpected url {url}")

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

    out = tmp_path / "poland_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 2  # the 404 KRS number contributes no row

    active = df.filter(pl.col("registration_number") == "0000006865").row(0, named=True)
    assert active["company_name"] == "CD PROJEKT SPOLKA AKCYJNA"
    assert active["registered_address"] == "JAGIELLONSKA, 74, 03-301, WARSZAWA, POLSKA"
    assert active["status"] is None  # no dzial6 liquidation/dissolution section present
    assert active["ubo_names"] is None
    assert active["jurisdiction"] == "PL"

    liquidated = df.filter(pl.col("registration_number") == "0000500000").row(0, named=True)
    assert liquidated["company_name"] == "FUNDACJA SWIATOWEGO TYGODNIA PRZEDSIEBIORCZOSCI W LIKWIDACJI"
    assert liquidated["status"] == "W LIKWIDACJI"
    assert liquidated["ubo_names"] is None


def test_poland_krs_exits_nonzero_on_request_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_poland" in sys.modules:
        del sys.modules["update_poland"]
    mod = importlib.import_module("update_poland")

    monkeypatch.setattr(mod, "KRS_RANGE_START", 1)
    monkeypatch.setattr(mod, "KRS_RANGE_END", 4)
    monkeypatch.setattr(mod, "MISS_SLEEP_SECONDS", 0)

    def _boom(url, **kwargs):
        raise requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "poland_reg.parquet").exists()


def test_poland_krs_exits_nonzero_on_zero_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_poland" in sys.modules:
        del sys.modules["update_poland"]
    mod = importlib.import_module("update_poland")

    monkeypatch.setattr(mod, "KRS_RANGE_START", 1)
    monkeypatch.setattr(mod, "KRS_RANGE_END", 3)
    monkeypatch.setattr(mod, "MISS_SLEEP_SECONDS", 0)

    def _all_404(url, **kwargs):
        raise _FakeNotFound()

    monkeypatch.setattr(mod, "get_with_retry", _all_404)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "poland_reg.parquet").exists()
