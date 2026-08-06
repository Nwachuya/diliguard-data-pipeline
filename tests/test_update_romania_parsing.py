"""Fixture-based test of the real ONRC (Romania Trade Register) parsing logic,
plus the fail-closed guarantee, in ONE file (mirrors test_update_slovakia_parsing.py's
convention, adapted for Romania's bulk-CSV-download shape like Estonia/Latvia).

Fixture shapes below mirror the REAL ONRC open-data column layout observed live:
- OD_FIRME.CSV: DENUMIRE^CUI^COD_INMATRICULARE^DATA_INMATRICULARE^EUID^FORMA_JURIDICA^
  ADR_TARA^ADR_JUDET^ADR_LOCALITATE^ADR_DEN_STRADA^ADR_NR_STRADA^ADR_BLOC^ADR_SCARA^
  ADR_ETAJ^ADR_APARTAMENT^ADR_COD_POSTAL^ADR_SECTOR^ADR_COMPLETARE^WEB^TARA_FIRMA_MAMA
- OD_STARE_FIRMA.CSV: COD_INMATRICULARE^COD (numeric status code)
- N_STARE_FIRMA.CSV: COD^DENUMIRE (decodes the numeric status code to a real Romanian
  label — confirmed live: 1048="funcțiune", 1084="radiată")
"""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

FIRME_HEADER = (
    "DENUMIRE^CUI^COD_INMATRICULARE^DATA_INMATRICULARE^EUID^FORMA_JURIDICA^ADR_TARA^"
    "ADR_JUDET^ADR_LOCALITATE^ADR_DEN_STRADA^ADR_NR_STRADA^ADR_BLOC^ADR_SCARA^ADR_ETAJ^"
    "ADR_APARTAMENT^ADR_COD_POSTAL^ADR_SECTOR^ADR_COMPLETARE^WEB^TARA_FIRMA_MAMA\n"
)
FIRME_CSV = FIRME_HEADER + (
    'TEST FIXTURE ACTIVE SRL^12345678^J40/1000/2020^01/01/2020^ROONRC.J40/1000/2020^SRL^'
    'România^București^București Sectorul 1^Str. TEST^10^^^^010101^1^^^\n'
    'TEST FIXTURE STRUCK OFF SRL^0^J40/2000/2015^02/02/2015^ROONRC.J40/2000/2015^SRL^'
    'România^Cluj^Cluj-Napoca^Str. NOTEST^5^^^^400001^^^^\n'
    'TEST FIXTURE UNKNOWN STATUS SRL^99999999^J40/3000/2019^03/03/2019^ROONRC.J40/3000/2019^SRL^'
    'România^Iași^Iași^Str. NOSTATUS^1^^^^700001^^^^\n'
)

STARE_FIRMA_CSV = (
    "COD_INMATRICULARE^COD\n"
    "J40/1000/2020^1048\n"
    "J40/2000/2015^1084\n"
)

N_STARE_FIRMA_CSV = (
    "COD^DENUMIRE\n"
    "1048^funcțiune\n"
    "1084^radiată\n"
    "2069^sediu expirat\n"
)


def test_romania_parses_real_shape_and_joins_status(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_romania" in sys.modules:
        del sys.modules["update_romania"]
    mod = importlib.import_module("update_romania")

    fake_firme_pkg = {
        "name": "firme-fixture-test",
        "metadata_modified": "2026-07-08T11:04:15.992179",
        "resources": [
            {"name": "OD_FIRME.CSV", "url": "https://data.gov.ro/fixture/od_firme.csv"},
            {"name": "OD_STARE_FIRMA.CSV", "url": "https://data.gov.ro/fixture/od_stare_firma.csv"},
        ],
    }
    fake_nomenclator_pkg = {
        "name": "nomenclatoare-fixture-test",
        "metadata_modified": "2026-07-08T10:57:48.303999",
        "resources": [
            {"name": "N_STARE_FIRMA.CSV", "url": "https://data.gov.ro/fixture/n_stare_firma.csv"},
        ],
    }

    def _fake_find_latest_package(query, title_prefix):
        if title_prefix.lower().startswith("firme"):
            return fake_firme_pkg
        return fake_nomenclator_pkg

    def _fake_download_firme_csv(url, local_path):
        Path(local_path).write_text(FIRME_CSV, encoding="utf-8")

    def _fake_download_file(url, local_path, **kwargs):
        if "od_stare_firma" in url:
            Path(local_path).write_text(STARE_FIRMA_CSV, encoding="utf-8")
        elif "n_stare_firma" in url:
            Path(local_path).write_text(N_STARE_FIRMA_CSV, encoding="utf-8")
        else:
            raise AssertionError(f"unexpected download_file call: {url}")

    monkeypatch.setattr(mod, "_find_latest_package", _fake_find_latest_package)
    monkeypatch.setattr(mod, "_download_firme_csv", _fake_download_firme_csv)
    monkeypatch.setattr(mod, "download_file", _fake_download_file)

    mod.main()

    out = tmp_path / "romania_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 3

    active = df.filter(pl.col("registration_number") == "J40/1000/2020").row(0, named=True)
    assert active["company_name"] == "TEST FIXTURE ACTIVE SRL"
    assert active["status"] == "funcțiune"
    assert active["ubo_names"] is None
    assert "București" in active["registered_address"]
    assert active["jurisdiction"] == "RO"

    struck_off = df.filter(pl.col("registration_number") == "J40/2000/2015").row(0, named=True)
    assert struck_off["status"] == "radiată"

    unknown = df.filter(pl.col("registration_number") == "J40/3000/2019").row(0, named=True)
    assert unknown["status"] is None  # no entry in OD_STARE_FIRMA fixture -> honest None, not invented


def test_romania_exits_nonzero_and_writes_nothing_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_romania" in sys.modules:
        del sys.modules["update_romania"]
    mod = importlib.import_module("update_romania")

    fake_firme_pkg = {
        "name": "firme-fixture-test",
        "metadata_modified": "2026-07-08T11:04:15.992179",
        "resources": [
            {"name": "OD_FIRME.CSV", "url": "https://data.gov.ro/fixture/od_firme.csv"},
            {"name": "OD_STARE_FIRMA.CSV", "url": "https://data.gov.ro/fixture/od_stare_firma.csv"},
        ],
    }
    fake_nomenclator_pkg = {
        "name": "nomenclatoare-fixture-test",
        "metadata_modified": "2026-07-08T10:57:48.303999",
        "resources": [
            {"name": "N_STARE_FIRMA.CSV", "url": "https://data.gov.ro/fixture/n_stare_firma.csv"},
        ],
    }

    def _fake_find_latest_package(query, title_prefix):
        if title_prefix.lower().startswith("firme"):
            return fake_firme_pkg
        return fake_nomenclator_pkg

    monkeypatch.setattr(mod, "_find_latest_package", _fake_find_latest_package)

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "download_file", _boom)
    monkeypatch.setattr(mod, "_download_firme_csv", lambda url, path: _boom())

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "romania_reg.parquet").exists()
