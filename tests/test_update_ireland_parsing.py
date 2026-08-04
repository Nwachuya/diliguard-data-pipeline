"""Fixture-based test of the real Ireland CRO parsing path (no network access)."""
import importlib
import io
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

FIXTURE_CSV = (
    "company_num,company_name,company_status_code,company_status,company_type_code,"
    "company_type,company_reg_date,last_ar_date,company_address_1,company_address_2,"
    "company_address_3,company_address_4,comp_dissolved_date,nard,last_accounts_date,"
    "company_status_date,nace_v2_code,eircode,company_name_eff_date,"
    "company_type_eff_date,princ_object_code\n"
    "999999,TEST FIXTURE LIMITED,1151,Normal ,1153,"
    "LTD - Private Company Limited by Shares,2020-01-01,2026-01-01,1 Test Street,,"
    ",Dublin 2,,2027-01-01,,,6202.0,D02 TEST,2020-01-01,2020-01-01,\n"
)


def _fixture_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("companies.csv", FIXTURE_CSV)
    return buf.getvalue()


def test_ireland_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_ireland" in sys.modules:
        del sys.modules["update_ireland"]
    mod = importlib.import_module("update_ireland")

    def _fake_download(url, local_path, **kwargs):
        Path(local_path).write_bytes(_fixture_zip_bytes())

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "ireland_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["company_name"] == "TEST FIXTURE LIMITED"
    assert row["registration_number"] == "999999"
    assert row["jurisdiction"] == "IE"
    assert row["status"] == "Normal"
    assert row["registered_address"] == "1 Test Street, Dublin 2, D02 TEST"
    assert row["ubo_names"] is None  # RBO is a separately-gated register, out of scope
    assert row["source_url"] == mod.COMPANIES_ZIP_URL
    assert row["fetched_at"] is not None


def test_ireland_exits_nonzero_and_writes_nothing_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_ireland" in sys.modules:
        del sys.modules["update_ireland"]
    mod = importlib.import_module("update_ireland")

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "download_file", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "ireland_reg.parquet").exists()
