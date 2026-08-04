"""Fixture-based test of the real Lithuania JAR parsing/pagination logic (no network)."""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# Real response shape observed live from
# https://get.data.gov.lt/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo
# with select(ja_kodas,ja_pavadinimas,pilnas_adresas,adresas,statusas.pavadinimas,
# forma.pavadinimas) — the dot-notation expand genuinely resolves the statusas/forma
# ref UUIDs to real human-readable Lithuanian strings.
PAGE_1 = {
    "_data": [
        {
            "ja_kodas": 110000291,
            "ja_pavadinimas": 'Bendra Lietuvos - Jungtinių Amerikos Valstijų įmonė uždaroji akcinė bendrovė "STT Inc."',
            "pilnas_adresas": None,
            "adresas": None,
            "statusas": {"pavadinimas": "Išregistruotas"},
            "forma": {"pavadinimas": "Uždaroji akcinė bendrovė"},
        },
        {
            "ja_kodas": 110000334,
            "ja_pavadinimas": 'Bendra Lietuvos - Vokietijos - Lenkijos įmonė uždaroji akcinė bendrovė "RUDOMEX"',
            "pilnas_adresas": None,
            "adresas": None,
            "statusas": {"pavadinimas": "Išregistruotas"},
            "forma": {"pavadinimas": "Uždaroji akcinė bendrovė"},
        },
    ],
    "_page": {"next": "WzExMDAwMDMzNCwgImRjMDE1MGRiLTkyZDQtNDJjOS1hOTNmLWViYmFmNmI5YjViMiJd"},
}
PAGE_2 = {
    "_data": [
        {
            "ja_kodas": 300123456,
            "ja_pavadinimas": "UAB Test Fixture Aktyvi",
            "pilnas_adresas": "Gedimino pr. 1, Vilnius",
            "adresas": None,
            "statusas": {"pavadinimas": "Įregistruotas"},
            "forma": {"pavadinimas": "Uždaroji akcinė bendrovė"},
        },
    ],
    # No _page.next key -> end of crawl.
}


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_lithuania_pagination_and_field_resolution(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_lithuania" in sys.modules:
        del sys.modules["update_lithuania"]
    mod = importlib.import_module("update_lithuania")

    pages = [PAGE_1, PAGE_2]

    def _fake_get(url, **kwargs):
        return _FakeResponse(pages.pop(0))

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

    out = tmp_path / "lithuania_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 3

    deregistered = df.filter(pl.col("registration_number") == 110000291).row(0, named=True)
    assert deregistered["company_name"] == 'Bendra Lietuvos - Jungtinių Amerikos Valstijų įmonė uždaroji akcinė bendrovė "STT Inc."'
    assert deregistered["status"] == "Išregistruotas"
    assert deregistered["registered_address"] is None
    assert deregistered["ubo_names"] is None
    assert deregistered["jurisdiction"] == "LT"

    active = df.filter(pl.col("registration_number") == 300123456).row(0, named=True)
    assert active["company_name"] == "UAB Test Fixture Aktyvi"
    assert active["status"] == "Įregistruotas"
    assert active["registered_address"] == "Gedimino pr. 1, Vilnius"


def test_lithuania_exits_nonzero_on_request_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_lithuania" in sys.modules:
        del sys.modules["update_lithuania"]
    mod = importlib.import_module("update_lithuania")

    def _boom(url, **kwargs):
        raise mod.requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "lithuania_reg.parquet").exists()
