"""Fixture-based test of the real UK Companies House parsing path (no network)."""
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
    "CompanyName, CompanyNumber,RegAddress.CareOf,RegAddress.POBox,RegAddress.AddressLine1, "
    "RegAddress.AddressLine2,RegAddress.PostTown,RegAddress.County,RegAddress.Country,RegAddress.PostCode,"
    "CompanyCategory,CompanyStatus\n"
    '"TEST FIXTURE LTD","08209948","","","9 TEST STREET","","TESTTOWN","","ENGLAND","TE1 1ST",'
    '"Private Limited Company","Active"\n'
)


def _make_fixture_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("BasicCompanyDataAsOneFile-2026-08-01.csv", FIXTURE_CSV)


class _FakeIndexResponse:
    text = '<a href="BasicCompanyDataAsOneFile-2026-08-01.zip">download</a>'


def test_uk_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_uk_companies_house" in sys.modules:
        del sys.modules["update_uk_companies_house"]
    mod = importlib.import_module("update_uk_companies_house")

    monkeypatch.setattr(mod, "get_with_retry", lambda *a, **kw: _FakeIndexResponse())

    def _fake_download(url, local_path, **kwargs):
        _make_fixture_zip(Path(local_path))

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "uk_companies_house.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["company_name"] == "TEST FIXTURE LTD"
    assert row["registration_number"] == "08209948"
    assert row["registered_address"] == "9 TEST STREET, TESTTOWN, ENGLAND, TE1 1ST"
    assert row["status"] == "Active"
    assert row["jurisdiction"] == "GB"
    assert row["ubo_names"] is None


def test_uk_exits_nonzero_when_index_page_unresolvable(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_uk_companies_house" in sys.modules:
        del sys.modules["update_uk_companies_house"]
    mod = importlib.import_module("update_uk_companies_house")

    class _NoLinkResponse:
        text = "<html>no zip link here</html>"

    monkeypatch.setattr(mod, "get_with_retry", lambda *a, **kw: _NoLinkResponse())

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "uk_companies_house.parquet").exists()
