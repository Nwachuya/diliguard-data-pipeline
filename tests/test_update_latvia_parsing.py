"""Fixture-based test of the real Latvia parsing path (no network access)."""
import importlib
import sys
from pathlib import Path

import polars as pl

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

FIXTURE_CSV = (
    "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;without_quotes;"
    "regtype;regtype_text;type;type_text;registered;terminated;closed;address;index;addressid;"
    "region;city;atvk;reregistration_term\n"
    '40003000999;LV00ZZZ40003000999;"TEST FIXTURE SIA";;TEST FIXTURE SIA;;0;K;Komercregistrs;'
    "SIA;Sabiedriba ar ierobezotu atbildibu;2001-01-01;;;Riga, Test iela 1;1000;1;0;0;;\n"
)


def test_latvia_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_latvia_ur" in sys.modules:
        del sys.modules["update_latvia_ur"]
    mod = importlib.import_module("update_latvia_ur")

    def _fake_download(url, local_path, **kwargs):
        Path(local_path).write_text(FIXTURE_CSV, encoding="utf-8")

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "latvia_ur.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["company_name"] == "TEST FIXTURE SIA"
    assert row["registration_number"] == "40003000999"
    assert row["jurisdiction"] == "LV"
    assert row["status"] == "ACTIVE"
    assert row["source_url"] == mod.REGISTER_CSV_URL
    assert row["fetched_at"] is not None
