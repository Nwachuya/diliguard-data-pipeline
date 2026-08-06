"""Fixture-based test of the real Bulgaria Commercial Register crawl (no network) —
mirrors the shape of the live Deeds/Summary JSON endpoint (including its real
`count` response header, used by the recursive prefix-partition crawl to decide
whether to page a prefix fully or split it further) captured during development.
"""
import importlib
import json
import sys
from urllib.parse import parse_qs, urlparse

from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


class _FakeResponse:
    def __init__(self, text, headers=None):
        self.text = text
        self.headers = headers or {}

    def json(self):
        import json
        return json.loads(self.text)


def _import_fresh():
    if "update_bulgaria" in sys.modules:
        del sys.modules["update_bulgaria"]
    return importlib.import_module("update_bulgaria")


def _query(url):
    return {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}


def test_bulgaria_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["ТЕСТ"])

    page_1 = (
        '[{"isPhysical":false,"ident":"831902088","name":"ТЕСТ","companyFullName":"\\"ТЕСТ\\" АД"},'
        '{"isPhysical":false,"ident":"175346309","name":"ТЕСТ БИЛДИНГС","companyFullName":"\\"ТЕСТ БИЛДИНГС\\" АДСИЦ"}]'
    )

    def _fake_get(url, **kwargs):
        q = _query(url)
        if q.get("pageSize") == "1":
            return _FakeResponse("[]", headers={"count": "2"})
        if q.get("page") == "1":
            return _FakeResponse(page_1)
        return _FakeResponse("")  # real observed "past end of results" shape: empty body

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main([])

    out = tmp_path / "bulgaria_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 2
    row = df.row(0, named=True)
    assert row["company_name"] == '"ТЕСТ" АД'
    assert row["registration_number"] == "831902088"
    assert row["jurisdiction"] == "BG"
    assert row["registered_address"] is None
    assert row["status"] is None
    assert row["ubo_names"] is None


def test_bulgaria_subdivides_prefix_that_exceeds_page_cap(monkeypatch, tmp_path):
    """The core recursive-partitioning behaviour: a seed prefix whose real total
    (from the `count` header) exceeds this run's MAX_RESULTS_PER_PREFIX must be
    split into child prefixes (seed + each alphabet character) instead of being
    paged directly — mirroring update_czech.py's ARES trie-partition crawl."""
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A", "B"])
    monkeypatch.setattr(mod, "MAX_RESULTS_PER_PREFIX", 2)
    monkeypatch.setattr(mod, "MAX_PREFIX_DEPTH", 2)

    # Real totals per prefix: "A" and "B" alone exceed the (tiny, test-only) cap of 2,
    # forcing subdivision into "AA"/"AB" and "BA"/"BB", each of which is small enough
    # to page fully.
    totals = {"A": 5, "B": 5, "AA": 1, "AB": 1, "BA": 1, "BB": 1}
    single_item = lambda ident, name: (
        f'[{{"isPhysical":false,"ident":"{ident}","name":"{name}","companyFullName":"{name} EOOD"}}]'
    )

    fetched_prefixes_at_page_size_25 = []

    def _fake_get(url, **kwargs):
        q = _query(url)
        name = q["name"]
        if q.get("pageSize") == "1":
            return _FakeResponse("[]", headers={"count": str(totals.get(name, 0))})
        # pageSize=25 leaf paging — should only ever be called for the fully-subdivided
        # leaves ("AA","AB","BA","BB"), never for "A"/"B" directly.
        fetched_prefixes_at_page_size_25.append(name)
        if q.get("page") == "1":
            return _FakeResponse(single_item(f"ID-{name}", name))
        return _FakeResponse("")

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main([])

    out = tmp_path / "bulgaria_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 4  # one real row from each of the 4 leaf partitions
    assert set(df["company_name"]) == {"AA EOOD", "AB EOOD", "BA EOOD", "BB EOOD"}
    # Confirms the crawl actually subdivided rather than trying to page "A"/"B" directly.
    assert "A" not in fetched_prefixes_at_page_size_25
    assert "B" not in fetched_prefixes_at_page_size_25
    assert set(fetched_prefixes_at_page_size_25) == {"AA", "AB", "BA", "BB"}


def test_bulgaria_warns_and_page_caps_when_still_over_cap_at_max_depth(monkeypatch, tmp_path, capsys):
    """At MAX_PREFIX_DEPTH, a prefix still exceeding the cap must not recurse forever —
    it logs a WARNING and pages only the first MAX_RESULTS_PER_PREFIX rows (a real,
    disclosed gap), matching update_czech.py's depth-cap behaviour."""
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A"])
    monkeypatch.setattr(mod, "MAX_RESULTS_PER_PREFIX", 1)
    monkeypatch.setattr(mod, "MAX_PREFIX_DEPTH", 1)  # "A" is already at max depth

    def _fake_get(url, **kwargs):
        q = _query(url)
        if q.get("pageSize") == "1":
            return _FakeResponse("[]", headers={"count": "999"})  # still "too many" at max depth
        if q.get("page") == "1":
            return _FakeResponse('[{"isPhysical":false,"ident":"1","name":"A CO","companyFullName":"A CO EOOD"}]')
        return _FakeResponse("")

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main([])

    out = tmp_path / "bulgaria_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    captured = capsys.readouterr()
    assert "WARNING" in captured.err
    assert "'A'" in captured.err


def test_bulgaria_exits_nonzero_when_response_shape_unexpected(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["ТЕСТ"])

    def _fake_get(url, **kwargs):
        q = _query(url)
        if q.get("pageSize") == "1":
            return _FakeResponse("[]", headers={"count": "1"})
        # Missing the 'ident'/'name' keys the parser depends on — simulates a
        # backend redesign, not a legitimate "no results" answer.
        return _FakeResponse('[{"unexpectedField": "value"}]')

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1
    assert not (tmp_path / "bulgaria_reg.parquet").exists()


def test_bulgaria_exits_nonzero_on_network_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["ТЕСТ"])

    def _boom(url, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main([])
    assert exc_info.value.code == 1
    assert not (tmp_path / "bulgaria_reg.parquet").exists()


def _make_fetcher(totals: dict, items_by_prefix: dict):
    """Build a fake `get_with_retry` for a flat (non-subdividing) set of seed
    prefixes: `totals` gives each prefix's real total (kept under whatever
    MAX_RESULTS_PER_PREFIX is active so every prefix pages fully as a single leaf),
    `items_by_prefix` gives the single page-1 JSON body for each prefix."""
    def _fake_get(url, **kwargs):
        q = _query(url)
        name = q["name"]
        if q.get("pageSize") == "1":
            return _FakeResponse("[]", headers={"count": str(totals.get(name, 0))})
        if q.get("page") == "1":
            return _FakeResponse(items_by_prefix.get(name, "[]"))
        return _FakeResponse("")
    return _fake_get


def test_bulgaria_checkpoint_resume_skips_completed_and_accumulates_rows(monkeypatch, tmp_path):
    """Core checkpoint/resume behaviour: run 1 (bounded to 2 seed prefixes via
    BULGARIA_MAX_PREFIXES_PER_RUN) crawls only "A" and "B", writes their rows, and
    persists a state file marking them completed. Run 2, with the same state file
    present, must skip "A"/"B" and crawl only the remaining "C"/"D" — and the final
    parquet must contain the accumulated rows from ALL FOUR prefixes, not just the
    latest run's two, proving the previous run's rows were preserved/merged rather
    than overwritten."""
    monkeypatch.chdir(tmp_path)

    totals = {"A": 1, "B": 1, "C": 1, "D": 1}
    items = {
        "A": '[{"isPhysical":false,"ident":"ID-A","name":"A","companyFullName":"A EOOD"}]',
        "B": '[{"isPhysical":false,"ident":"ID-B","name":"B","companyFullName":"B EOOD"}]',
        "C": '[{"isPhysical":false,"ident":"ID-C","name":"C","companyFullName":"C EOOD"}]',
        "D": '[{"isPhysical":false,"ident":"ID-D","name":"D","companyFullName":"D EOOD"}]',
    }

    # --- Run 1: only "A" and "B" should be attempted.
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A", "B", "C", "D"])
    monkeypatch.setattr(mod, "MAX_PREFIXES_PER_RUN", 2)
    fetched_run1 = []

    def _fake_get_run1(url, **kwargs):
        q = _query(url)
        if q.get("pageSize") != "1":
            fetched_run1.append(q["name"])
        return _make_fetcher(totals, items)(url, **kwargs)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get_run1)
    mod.main([])

    assert set(fetched_run1) == {"A", "B"}, "run 1 should only crawl the first 2 pending prefixes"

    out = tmp_path / "bulgaria_reg.parquet"
    assert out.exists()
    df1 = pl.read_parquet(out)
    assert df1.height == 2
    assert set(df1["company_name"]) == {"A EOOD", "B EOOD"}

    state_path = tmp_path / "bulgaria_crawl_state.json"
    assert state_path.exists()
    import json
    state = json.loads(state_path.read_text())
    assert set(state["completed_prefixes"]) == {"A", "B"}
    assert state["cycle"] == 1

    # --- Run 2: fresh module import (simulates a new CI job), same state file on
    # disk. Must skip "A"/"B" and crawl only "C"/"D".
    mod2 = _import_fresh()
    monkeypatch.setattr(mod2, "SEED_ALPHABET", ["A", "B", "C", "D"])
    monkeypatch.setattr(mod2, "MAX_PREFIXES_PER_RUN", 2)
    fetched_run2 = []

    def _fake_get_run2(url, **kwargs):
        q = _query(url)
        if q.get("pageSize") != "1":
            fetched_run2.append(q["name"])
        return _make_fetcher(totals, items)(url, **kwargs)

    monkeypatch.setattr(mod2, "get_with_retry", _fake_get_run2)
    mod2.main([])

    assert set(fetched_run2) == {"C", "D"}, "run 2 must skip already-completed A/B and crawl only C/D"

    df2 = pl.read_parquet(out)
    assert df2.height == 4, "final parquet must contain accumulated rows from all 4 prefixes, not just run 2's"
    assert set(df2["company_name"]) == {"A EOOD", "B EOOD", "C EOOD", "D EOOD"}

    state2 = json.loads(state_path.read_text())
    assert set(state2["completed_prefixes"]) == {"A", "B", "C", "D"}
    assert state2["cycle"] == 1


def test_bulgaria_wraps_around_to_new_cycle_once_alphabet_complete(monkeypatch, tmp_path):
    """Once every seed prefix is marked completed, the next run must reset
    completed_prefixes and bump the cycle counter rather than reporting
    'nothing pending' forever."""
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()

    state_path = tmp_path / "bulgaria_crawl_state.json"
    state_path.write_text(json.dumps({
        "completed_prefixes": ["A", "B"], "last_run": "2026-01-01T00:00:00+00:00", "cycle": 1,
    }))

    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A", "B"])
    totals = {"A": 1, "B": 1}
    items = {
        "A": '[{"isPhysical":false,"ident":"ID-A2","name":"A","companyFullName":"A EOOD"}]',
        "B": '[{"isPhysical":false,"ident":"ID-B2","name":"B","companyFullName":"B EOOD"}]',
    }
    monkeypatch.setattr(mod, "get_with_retry", _make_fetcher(totals, items))

    mod.main([])

    state = json.loads(state_path.read_text())
    assert state["cycle"] == 2, "a fully-completed pass must start a fresh cycle, not freeze"
    assert set(state["completed_prefixes"]) == {"A", "B"}, "the fresh cycle re-crawled and re-completed both seeds"


def test_bulgaria_run_never_writes_or_updates_state_on_mid_crawl_failure(monkeypatch, tmp_path):
    """A failure partway through a run's pending prefixes must not corrupt either
    the previously-accumulated output parquet or the previously-saved state file —
    fail-closed, not a partial/corrupt write."""
    monkeypatch.chdir(tmp_path)

    totals = {"A": 1, "B": 1}
    items = {"A": '[{"isPhysical":false,"ident":"ID-A","name":"A","companyFullName":"A EOOD"}]'}

    # --- Run 1: successfully crawl and persist "A" only.
    mod = _import_fresh()
    monkeypatch.setattr(mod, "SEED_ALPHABET", ["A", "B"])
    monkeypatch.setattr(mod, "MAX_PREFIXES_PER_RUN", 1)
    monkeypatch.setattr(mod, "get_with_retry", _make_fetcher(totals, items))
    mod.main([])

    out = tmp_path / "bulgaria_reg.parquet"
    state_path = tmp_path / "bulgaria_crawl_state.json"
    before_parquet_bytes = out.read_bytes()
    before_state_text = state_path.read_text()

    # --- Run 2: "B" is now pending, but the network call for it fails.
    mod2 = _import_fresh()
    monkeypatch.setattr(mod2, "SEED_ALPHABET", ["A", "B"])

    def _boom(url, **kwargs):
        raise RuntimeError("simulated mid-crawl network failure")

    monkeypatch.setattr(mod2, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod2.main([])
    assert exc_info.value.code == 1

    assert out.read_bytes() == before_parquet_bytes, "previously accumulated parquet must be untouched"
    assert state_path.read_text() == before_state_text, "previously saved state file must be untouched"
