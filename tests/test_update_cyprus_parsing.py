"""Fixture-based test of the real Cyprus DRCOR efiling search parsing path (no
network) — the fixture HTML mirrors the real results grid's structure captured
during development (table id="ctl00_cphMyMasterCentral_GridView1", 7 columns,
plus the "no results"/"too many results" label ids used to detect the other two
legitimate page shapes).
"""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


class _FakeResponse:
    def __init__(self, text):
        self.text = text


RESULTS_PAGE_HTML = """
<html><body>
<table id="ctl00_cphMyMasterCentral_GridView1">
  <tr><th></th><th>Όνομα</th><th>-</th><th>Αριθμός Εγγραφής</th><th>Τύπος</th><th>Κατάσταση Ονόματος</th><th>Κατάσταση Οργανισμού</th></tr>
  <tr><td>Select</td><td>TEST FIXTURE LIMITED</td><td>HE</td><td>123456</td><td>Εταιρεία</td><td>Τελευταίο Όνομα</td><td>Εγγεγραμμένη</td></tr>
  <tr><td>Select</td><td>TEST FIXTURE APPLICATION LTD</td><td></td><td></td><td></td><td>Αίτηση Ονόματος</td><td></td></tr>
</table>
</body></html>
"""

NO_RESULTS_HTML = """
<html><body>
<span id="ctl00_cphMyMasterCentral_lblNoSearchResults">Δεν βρέθηκαν αποτελέσματα</span>
</body></html>
"""

UNRECOGNIZABLE_HTML = """
<html><body><p>Something else entirely — no grid, no labels.</p></body></html>
"""


def _import_fresh():
    if "update_cyprus" in sys.modules:
        del sys.modules["update_cyprus"]
    return importlib.import_module("update_cyprus")


def test_cyprus_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "DEFAULT_SEARCH_TERMS", ["TEST FIXTURE"])

    def _fake_get(url, **kwargs):
        if "index=1" in url:
            return _FakeResponse(RESULTS_PAGE_HTML)
        return _FakeResponse(NO_RESULTS_HTML)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

    out = tmp_path / "cyprus_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    # The name-reservation-application row (no registration number) must be skipped.
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["company_name"] == "TEST FIXTURE LIMITED"
    assert row["registration_number"] == "HE123456"
    assert row["status"] == "Εγγεγραμμένη"
    assert row["jurisdiction"] == "CY"
    assert row["registered_address"] is None
    assert row["ubo_names"] is None


def test_cyprus_exits_nonzero_when_page_shape_unrecognizable(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "DEFAULT_SEARCH_TERMS", ["TEST FIXTURE"])

    def _fake_get(url, **kwargs):
        return _FakeResponse(UNRECOGNIZABLE_HTML)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "cyprus_reg.parquet").exists()


def test_cyprus_exits_nonzero_on_network_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "DEFAULT_SEARCH_TERMS", ["TEST FIXTURE"])

    def _boom(url, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "cyprus_reg.parquet").exists()
