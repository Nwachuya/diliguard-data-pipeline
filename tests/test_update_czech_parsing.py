"""Fixture-based test of the real Czech ARES parsing/partitioning logic (no network)."""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Real single-lookup/list-item shape confirmed live via curl against
# https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/{ico} and
# .../vyhledat (list items have the identical shape).
ACTIVE_SUBJECT = {
    "ico": "27074358",
    "obchodniJmeno": "Asseco Central Europe, a.s.",
    "sidlo": {
        "kodStatu": "CZ",
        "textovaAdresa": "Budějovická 778/3a, Michle, 14000 Praha 4",
    },
    "pravniForma": "121",
    "pravniFormaRos": "121",
    "datumVzniku": "2003-08-06",
    "seznamRegistraci": {
        "stavZdrojeRos": "AKTIVNI",
        "stavZdrojeVr": "AKTIVNI",
        "stavZdrojeRzp": "AKTIVNI",
        "stavZdrojeNrpzs": "NEEXISTUJICI",
    },
}
TERMINATED_SUBJECT = {
    "ico": "11111111",
    "obchodniJmeno": "TEST FIXTURE ZANIKLA a.s.",
    "sidlo": {"kodStatu": "CZ", "textovaAdresa": "Testovací 1, 10000 Praha"},
    "pravniForma": "121",
    "datumVzniku": "1998-01-01",
    "datumZaniku": "2020-01-01",
    "seznamRegistraci": {"stavZdrojeRos": "ZANIKLY"},
}
NO_STATUS_SUBJECT = {
    "ico": "22222222",
    "obchodniJmeno": "TEST FIXTURE NO STATUS s.r.o.",
    "sidlo": {"kodStatu": "CZ", "textovaAdresa": None},
    "pravniForma": "112",
    "seznamRegistraci": {},
}

TOO_MANY_RESULTS_ERROR = {
    "kod": "CHYBA_VSTUPU",
    "popis": "Zadaný dotaz vrací příliš mnoho výsledků (581 859). Povoleno je maximálně 1 000 výsledků.",
    "subKod": "VYSTUP_PRILIS_MNOHO_VYSLEDKU",
}


def _fresh_module():
    if "update_czech" in sys.modules:
        del sys.modules["update_czech"]
    return importlib.import_module("update_czech")


def test_czech_partitioning_and_parsing(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    # Simulate a tiny universe: prefix "A" has too many results and must be split
    # into children; prefix "T" resolves directly to a small leaf; every other
    # single-letter seed has zero matches.
    def _fake_search(body, **kwargs):
        prefix = body.get("obchodniJmeno", "")
        start = body.get("start", 0)

        if prefix == "A":
            return dict(TOO_MANY_RESULTS_ERROR)
        if prefix == "AS":
            # One further split needed.
            return dict(TOO_MANY_RESULTS_ERROR)
        if prefix == "ASS":
            if start == 0:
                return {"pocetCelkem": 1, "ekonomickeSubjekty": [ACTIVE_SUBJECT]}
            return {"pocetCelkem": 1, "ekonomickeSubjekty": []}
        if prefix == "T":
            if start == 0:
                return {"pocetCelkem": 2, "ekonomickeSubjekty": [TERMINATED_SUBJECT, NO_STATUS_SUBJECT]}
            return {"pocetCelkem": 2, "ekonomickeSubjekty": []}
        # Every other prefix (single letters/digits, and any other "A"-prefixed
        # child besides "AS") has zero real matches in this fixture universe.
        return {"pocetCelkem": 0, "ekonomickeSubjekty": []}

    monkeypatch.setattr(mod, "search_ares", _fake_search)

    mod.main([])

    out = tmp_path / "czech_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 3

    active = df.filter(pl.col("registration_number") == "27074358").row(0, named=True)
    assert active["company_name"] == "Asseco Central Europe, a.s."
    assert active["registered_address"] == "Budějovická 778/3a, Michle, 14000 Praha 4"
    assert active["status"] == "ACTIVE"
    assert active["ubo_names"] is None
    assert active["jurisdiction"] == "CZ"

    terminated = df.filter(pl.col("registration_number") == "11111111").row(0, named=True)
    assert terminated["status"] == "TERMINATED"  # driven by datumZaniku, not just stavZdrojeRos

    no_status = df.filter(pl.col("registration_number") == "22222222").row(0, named=True)
    assert no_status["status"] is None  # no fabricated status when the source has none
    assert no_status["registered_address"] is None


def test_czech_exits_nonzero_on_request_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    def _boom(body, **kwargs):
        raise mod.requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "search_ares", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1
    assert not (tmp_path / "czech_reg.parquet").exists()


def test_czech_exits_nonzero_on_zero_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    def _empty(body, **kwargs):
        return {"pocetCelkem": 0, "ekonomickeSubjekty": []}

    monkeypatch.setattr(mod, "search_ares", _empty)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1
    assert not (tmp_path / "czech_reg.parquet").exists()
