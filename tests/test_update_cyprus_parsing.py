"""Fixture-based test of the real Cyprus DRCOR efiling search crawl (no network) —
the fixture HTML mirrors the real results grid's structure captured during
development (table id="ctl00_cphMyMasterCentral_GridView1", 7 columns, plus the
"no results"/"too many results" label ids used to detect the other two
legitimate page shapes, and to drive the recursive prefix-partition crawl).
"""
import importlib
import sys
from urllib.parse import parse_qs, urlparse
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


class _FakeResponse:
    def __init__(self, text):
        self.text = text


def _grid_html(rows):
    """rows: list of (name, prefix, number, entity_type, name_status, org_status);
    number == "" means a name-reservation application (should be skipped)."""
    header = (
        "<tr><th></th><th>Όνομα</th><th>-</th><th>Αριθμός Εγγραφής</th>"
        "<th>Τύπος</th><th>Κατάσταση Ονόματος</th><th>Κατάσταση Οργανισμού</th></tr>"
    )
    body = "".join(
        f"<tr><td>Select</td><td>{name}</td><td>{prefix}</td><td>{number}</td>"
        f"<td>{entity_type}</td><td>{name_status}</td><td>{org_status}</td></tr>"
        for name, prefix, number, entity_type, name_status, org_status in rows
    )
    return f'<html><body><table id="ctl00_cphMyMasterCentral_GridView1">{header}{body}</table></body></html>'


NO_RESULTS_HTML = """
<html><body>
<span id="ctl00_cphMyMasterCentral_lblNoSearchResults">Δεν βρέθηκαν αποτελέσματα</span>
</body></html>
"""

TOO_MANY_HTML = """
<html><body>
<span id="ctl00_cphMyMasterCentral_lblResultsExceeded">Βρέθηκαν πολλά αποτελέσματα</span>
</body></html>
"""

UNRECOGNIZABLE_HTML = """
<html><body><p>Something else entirely — no grid, no labels.</p></body></html>
"""


def _import_fresh():
    if "update_cyprus" in sys.modules:
        del sys.modules["update_cyprus"]
    return importlib.import_module("update_cyprus")


def _query(url):
    return {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}


def test_cyprus_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["TEST FIXTURE"])

    results_html = _grid_html([
        ("TEST FIXTURE LIMITED", "HE", "123456", "Εταιρεία", "Τελευταίο Όνομα", "Εγγεγραμμένη"),
        ("TEST FIXTURE APPLICATION LTD", "", "", "", "Αίτηση Ονόματος", ""),
    ])

    def _fake_get(url, **kwargs):
        q = _query(url)
        if q.get("index") == "1":
            return _FakeResponse(results_html)
        return _FakeResponse(NO_RESULTS_HTML)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main([])

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


def test_cyprus_subdivides_prefix_that_the_site_flags_as_too_many_results(monkeypatch, tmp_path):
    """The core recursive-partitioning behaviour: a seed prefix the site flags with
    lblResultsExceeded must be split into child prefixes (seed + each alphabet
    character) instead of being treated as a dead end — mirroring
    update_czech.py's ARES trie-partition crawl, adapted to Cyprus's real
    'too many results' signal (which, unlike Czech, carries no exact count)."""
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A", "B"])
    monkeypatch.setattr(mod, "MAX_PREFIX_DEPTH", 2)

    exceeded_prefixes = {"A", "B"}
    leaf_row_html = {
        "AA": _grid_html([("AA CO LIMITED", "HE", "1", "Εταιρεία", "Τελευταίο Όνομα", "Εγγεγραμμένη")]),
        "AB": _grid_html([("AB CO LIMITED", "HE", "2", "Εταιρεία", "Τελευταίο Όνομα", "Εγγεγραμμένη")]),
        "BA": _grid_html([("BA CO LIMITED", "HE", "3", "Εταιρεία", "Τελευταίο Όνομα", "Εγγεγραμμένη")]),
        "BB": _grid_html([("BB CO LIMITED", "HE", "4", "Εταιρεία", "Τελευταίο Όνομα", "Εγγεγραμμένη")]),
    }

    fetched_first_pages = []

    def _fake_get(url, **kwargs):
        q = _query(url)
        name = q["name"]
        if q.get("index") == "1":
            fetched_first_pages.append(name)
        if name in exceeded_prefixes:
            return _FakeResponse(TOO_MANY_HTML)
        if q.get("index") == "1":
            return _FakeResponse(leaf_row_html[name])
        return _FakeResponse(NO_RESULTS_HTML)  # page 2 of any leaf: no more rows

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main([])

    out = tmp_path / "cyprus_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 4
    assert set(df["company_name"]) == {"AA CO LIMITED", "AB CO LIMITED", "BA CO LIMITED", "BB CO LIMITED"}
    # "A" and "B" themselves were probed (and flagged exceeded) but never yielded rows directly.
    assert set(fetched_first_pages) == {"A", "B", "AA", "AB", "BA", "BB"}


def test_cyprus_warns_when_still_exceeded_at_max_depth(monkeypatch, tmp_path, capsys):
    """At MAX_PREFIX_DEPTH, a prefix still flagged 'too many results' must not recurse
    forever — it logs a WARNING and contributes zero rows (a real, disclosed gap: the
    site itself refuses to serve any rows at that specificity)."""
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A"])
    monkeypatch.setattr(mod, "MAX_PREFIX_DEPTH", 1)  # "A" is already at max depth

    def _fake_get(url, **kwargs):
        return _FakeResponse(TOO_MANY_HTML)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1  # zero rows collected at all -> fail-closed, same as before
    captured = capsys.readouterr()
    assert "WARNING" in captured.err
    assert "'A'" in captured.err


def test_cyprus_exits_nonzero_when_page_shape_unrecognizable(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["TEST FIXTURE"])

    def _fake_get(url, **kwargs):
        return _FakeResponse(UNRECOGNIZABLE_HTML)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1
    assert not (tmp_path / "cyprus_reg.parquet").exists()


def test_cyprus_exits_nonzero_on_network_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["TEST FIXTURE"])

    def _boom(url, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1
    assert not (tmp_path / "cyprus_reg.parquet").exists()
