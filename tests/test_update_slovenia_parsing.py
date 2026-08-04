"""Fixture-based test of the real Slovenia (AJPES) parsing path (no network access)."""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

# UTF-16 with BOM, matching the real AJPES CSV's actual on-disk encoding.
FIXTURE_HEADER = (
    '"Matična številka","Popolno ime","HSEID","Pravnoorganizacijska oblika",'
    '"Registrski organ","Ulica","Hišna št ","Hišna št  dodatek","Naselje",'
    '"Poštna št ","Pošta","Država"\n'
)
FIXTURE_ROW = (
    '"5000152000","TEST FIXTURE D.O.O.","100400000155896552",'
    '"Družba z omejeno odgovornostjo","AJPES, izpostava Ljubljana","Testna ulica",'
    '"1","","Ljubljana","1000","Ljubljana","SLOVENIJA"\n'
)


def test_slovenia_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_slovenia" in sys.modules:
        del sys.modules["update_slovenia"]
    mod = importlib.import_module("update_slovenia")

    def _fake_download(url, local_path, **kwargs):
        with open(local_path, "w", encoding="utf-16") as f:
            f.write(FIXTURE_HEADER + FIXTURE_ROW)

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "slovenia_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["company_name"] == "TEST FIXTURE D.O.O."
    assert row["registration_number"] == "5000152000"
    assert row["jurisdiction"] == "SI"
    assert row["registered_address"] == "Testna ulica, 1, Ljubljana, 1000, Ljubljana, SLOVENIJA"
    assert row["status"] is None  # not published in this AJPES basic-data file
    assert row["ubo_names"] is None  # not published in Slovenia's open business register
    assert row["source_url"] == mod.REGISTER_CSV_URL
    assert row["fetched_at"] is not None


def test_slovenia_exits_nonzero_and_writes_nothing_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_slovenia" in sys.modules:
        del sys.modules["update_slovenia"]
    mod = importlib.import_module("update_slovenia")

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "download_file", _boom)
    monkeypatch.setattr(mod, "time", type("_T", (), {"sleep": staticmethod(lambda *_: None)}))

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "slovenia_reg.parquet").exists()
