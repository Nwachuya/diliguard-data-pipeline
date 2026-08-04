"""Fixture-based test of the real Bulgaria Commercial Register search parsing path
(no network) — mirrors the shape of the live Deeds/Summary JSON endpoint captured
during development.
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

    def json(self):
        import json
        return json.loads(self.text)


def _import_fresh():
    if "update_bulgaria" in sys.modules:
        del sys.modules["update_bulgaria"]
    return importlib.import_module("update_bulgaria")


def test_bulgaria_real_rows_parsed_from_fixture(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "DEFAULT_PREFIXES", ["ТЕСТ"])

    page_1 = (
        '[{"isPhysical":false,"ident":"831902088","name":"ТЕСТ","companyFullName":"\\"ТЕСТ\\" АД"},'
        '{"isPhysical":false,"ident":"175346309","name":"ТЕСТ БИЛДИНГС","companyFullName":"\\"ТЕСТ БИЛДИНГС\\" АДСИЦ"}]'
    )

    def _fake_get(url, **kwargs):
        if "page=1" in url:
            return _FakeResponse(page_1)
        return _FakeResponse("")  # real observed "past end of results" shape: empty body

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

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


def test_bulgaria_exits_nonzero_when_response_shape_unexpected(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "DEFAULT_PREFIXES", ["ТЕСТ"])

    def _fake_get(url, **kwargs):
        # Missing the 'ident'/'name' keys the parser depends on — simulates a
        # backend redesign, not a legitimate "no results" answer.
        return _FakeResponse('[{"unexpectedField": "value"}]')

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "bulgaria_reg.parquet").exists()


def test_bulgaria_exits_nonzero_on_network_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh()
    monkeypatch.setattr(mod, "DEFAULT_PREFIXES", ["ТЕСТ"])

    def _boom(url, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "bulgaria_reg.parquet").exists()
