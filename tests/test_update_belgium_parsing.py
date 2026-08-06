"""Fixture-based test of the Belgium KBO Open Data parsing logic (no real network,
no real KBO account). The login/download flow this exercises is UNTESTED against the
live site (see scripts/update_belgium.py docstring) — this only proves the CSV-join
parsing logic is correct given a response shaped like KBO's documented export format.
"""
import importlib
import io
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

ENTERPRISE_CSV = "EnterpriseNumber,Status,JuridicalSituation,TypeOfEnterprise,JuridicalForm\n0123456789,AC,000,2,014\n"
DENOMINATION_CSV = (
    "EntityNumber,Language,TypeOfDenomination,Denomination\n"
    "0123456789,2,001,TEST FIXTURE BVBA\n"
    "0123456789,2,003,TEST FIXTURE (abbrev)\n"
)
ADDRESS_CSV = (
    "EntityNumber,TypeOfAddress,CountryNL,CountryFR,Zipcode,MunicipalityNL,MunicipalityFR,"
    "StreetNL,StreetFR,HouseNumber,Box,ExtraAddressInfo,DateStrikingOff\n"
    "0123456789,REGO,,,1000,Brussel,Bruxelles,Teststraat,,1,,,\n"
)

FILES_PAGE_HTML = (
    '<html><body><a href="/kbo-open-data/download/KboOpenData_0001_2026_08_Full.zip">'
    "Full export</a></body></html>"
)
EXPORT_URL = "https://kbopub.economie.fgov.be/kbo-open-data/download/KboOpenData_0001_2026_08_Full.zip"


def _fixture_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("enterprise.csv", ENTERPRISE_CSV)
        z.writestr("denomination.csv", DENOMINATION_CSV)
        z.writestr("address.csv", ADDRESS_CSV)
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, *, text="", url="", content=b"", ok=True):
        self.text = text
        self.url = url
        self._content = content
        self.ok = ok

    def raise_for_status(self):
        if not self.ok:
            raise RuntimeError("simulated HTTP error")

    def iter_content(self, chunk_size=None):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeSession:
    """Stands in for requests.Session across the whole login -> list -> download flow."""

    def __init__(self, zip_bytes: bytes, fail_login: bool = False, fail_files_page: bool = False):
        self._zip_bytes = zip_bytes
        self._fail_login = fail_login
        self._fail_files_page = fail_files_page

    def get(self, url, **kwargs):
        if "login" in url:
            return _FakeResponse(url=url)
        if "files" in url:
            if self._fail_files_page:
                return _FakeResponse(text="<html>no links here</html>")
            return _FakeResponse(text=FILES_PAGE_HTML)
        if url == EXPORT_URL:
            return _FakeResponse(content=self._zip_bytes)
        raise AssertionError(f"unexpected GET {url}")

    def post(self, url, **kwargs):
        if self._fail_login:
            return _FakeResponse(url="https://kbopub.economie.fgov.be/kbo-open-data/login")
        return _FakeResponse(url="https://kbopub.economie.fgov.be/kbo-open-data/home")


def test_belgium_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KBO_USERNAME", "test-user")
    monkeypatch.setenv("KBO_PASSWORD", "test-pass")
    if "update_belgium" in sys.modules:
        del sys.modules["update_belgium"]
    mod = importlib.import_module("update_belgium")

    fake_session = _FakeSession(_fixture_zip_bytes())
    monkeypatch.setattr(mod.requests, "Session", lambda: fake_session)

    mod.main()

    out = tmp_path / "belgium_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["company_name"] == "TEST FIXTURE BVBA"
    assert row["registration_number"] == "0123456789"
    assert row["jurisdiction"] == "BE"
    assert row["status"] == "AC"
    assert row["registered_address"] == "Teststraat 1, 1000, Brussel"
    assert row["ubo_names"] is None  # KBO Open Data does not publish UBO data
    assert row["source_url"] == EXPORT_URL
    assert row["fetched_at"] is not None


def test_belgium_exits_nonzero_and_writes_nothing_without_credentials(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("KBO_USERNAME", raising=False)
    monkeypatch.delenv("KBO_PASSWORD", raising=False)
    if "update_belgium" in sys.modules:
        del sys.modules["update_belgium"]
    mod = importlib.import_module("update_belgium")

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "belgium_reg.parquet").exists()


def test_belgium_exits_nonzero_and_writes_nothing_on_login_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KBO_USERNAME", "test-user")
    monkeypatch.setenv("KBO_PASSWORD", "wrong-pass")
    if "update_belgium" in sys.modules:
        del sys.modules["update_belgium"]
    mod = importlib.import_module("update_belgium")

    fake_session = _FakeSession(_fixture_zip_bytes(), fail_login=True)
    monkeypatch.setattr(mod.requests, "Session", lambda: fake_session)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "belgium_reg.parquet").exists()
