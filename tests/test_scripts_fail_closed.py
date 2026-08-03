"""Guards against ever regressing to the old "network call fails -> fabricate rows"
pattern. Each script must exit non-zero and write nothing when its real data
source is unreachable — never fall back to hardcoded data.
"""
import importlib
import os
import sys
from pathlib import Path

import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


def _import_fresh(module_name: str):
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def test_update_estonia_exits_nonzero_and_writes_nothing_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh("update_estonia")

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "download_file", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "estonia_reg.parquet").exists()


def test_update_latvia_exits_nonzero_and_writes_nothing_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh("update_latvia_ur")

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "download_file", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "latvia_ur.parquet").exists()


def test_update_france_exits_nonzero_and_writes_nothing_on_query_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _import_fresh("update_france_rne")

    class _StubConnection:
        def execute(self, sql, *args, **kwargs):
            if sql.strip().startswith("COPY"):
                raise RuntimeError("simulated remote parquet read failure")
            return self

        def fetchone(self):
            return (0,)

    monkeypatch.setattr(mod.duckdb, "connect", lambda *a, **kw: _StubConnection())

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "france_rne.parquet").exists()
