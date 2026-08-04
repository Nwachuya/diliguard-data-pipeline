"""Fixture-based test of the real KVK Open Dataset parsing path (no network).

The fixture rows below mirror the REAL response shape observed from a live call
against https://www.kvk.nl/download/kvk-open-dataset-basis-bedrijfsgegevens.zip on
2026-08-04 (semicolon-delimited CSV, quoted fields, header:
"Datum aanvang";"Actief";"Insolventie";"Rechtsvorm";"Postcode regio";
"SBI activiteiten";"Hoofdactiviteiten";"Lidstaat"), e.g. real observed rows:
  "19720516";"J";"";"BV";"89";"64210,68203";"64210";"NL"
  "19540329";"N";"";"BV";"79";"6420";"6420";"NL"
  "20060721";"J";"SURS";"BV";"92";"47741,47742";"47741";"NL"
"""
import importlib
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

FIXTURE_CSV = (
    '"Datum aanvang";"Actief";"Insolventie";"Rechtsvorm";"Postcode regio";'
    '"SBI activiteiten";"Hoofdactiviteiten";"Lidstaat"\n'
    '"19720516";"J";"";"BV";"89";"64210,68203";"64210";"NL"\n'
    '"19540329";"N";"";"BV";"79";"6420";"6420";"NL"\n'
    '"20060721";"J";"SURS";"BV";"92";"47741,47742";"47741";"NL"\n'
)


def _make_fixture_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("kvk-open-dataset-basis-bedrijfsgegevens.csv", FIXTURE_CSV)


def test_netherlands_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_netherlands" in sys.modules:
        del sys.modules["update_netherlands"]
    mod = importlib.import_module("update_netherlands")

    def _fake_download(url, local_path, **kwargs):
        _make_fixture_zip(Path(local_path))

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "netherlands_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("status")
    assert df.height == 3

    # No identifier of any kind is ever present in this dataset — never fabricated.
    assert (df["company_name"].is_null()).all()
    assert (df["registration_number"].is_null()).all()
    assert (df["ubo_names"].is_null()).all()
    assert (df["jurisdiction"] == "NL").all()

    active = df.filter(pl.col("status") == "ACTIVE").row(0, named=True)
    assert active["registered_address"] == "postcode area: 89"

    inactive = df.filter(pl.col("status") == "INACTIVE").row(0, named=True)
    assert inactive["registered_address"] == "postcode area: 79"

    insolvent = df.filter(pl.col("status") == "SURS").row(0, named=True)
    assert insolvent["registered_address"] == "postcode area: 92"


def test_netherlands_exits_nonzero_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_netherlands" in sys.modules:
        del sys.modules["update_netherlands"]
    mod = importlib.import_module("update_netherlands")

    def _boom(url, local_path, **kwargs):
        raise RuntimeError("simulated download failure")

    monkeypatch.setattr(mod, "download_file", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "netherlands_reg.parquet").exists()
