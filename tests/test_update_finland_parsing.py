"""Fixture-based test of the real Finland PRH/YTJ bulk parsing logic (no network).

Fixtures below are trimmed-down real shapes observed from a live call to
https://avoindata.prh.fi/opendata-ytj-api/v3/all_companies on 2026-08-04 (fields not
needed for parsing logic have been dropped for brevity, but the field
names/nesting/language-code convention, and the fact this is a single JSON array
inside a single-member zip, are exactly what the live bulk endpoint returns).
"""
import importlib
import io
import json
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

COMPANIES = [
    {
        "businessId": {"value": "2283132-3", "registrationDate": "2009-08-27"},
        "names": [
            {"name": "Compater Oy", "type": "1", "registrationDate": "2009-09-14"}
        ],
        "addresses": [
            {
                "type": 2,
                "street": "Hyljetie",
                "postCode": "02260",
                "postOffices": [
                    {"city": "ESBO", "languageCode": "2"},
                    {"city": "ESPOO", "languageCode": "1"},
                ],
                "buildingNumber": "18",
                "entrance": "A",
            }
        ],
        "registeredEntries": [
            {
                "type": "0",
                "register": "1",
                "registrationDate": "2009-08-27",
                "endDate": "2009-09-13",
                "descriptions": [
                    {"languageCode": "3", "description": "Unregistered"},
                    {"languageCode": "1", "description": "Rekisteroimaton"},
                ],
            },
            {
                "type": "1",
                "register": "1",
                "registrationDate": "2009-09-14",
                "descriptions": [
                    {"languageCode": "3", "description": "Registered"},
                    {"languageCode": "1", "description": "Rekisterissa"},
                ],
            },
        ],
    },
    {
        "businessId": {"value": "0100002-9", "registrationDate": "1978-03-15"},
        "names": [
            {"name": "Artjarven Metalli Oy", "type": "1", "registrationDate": "1976-04-23", "endDate": "2005-12-19"}
        ],
        "addresses": [],
        "registeredEntries": [
            {
                "type": "1",
                "register": "1",
                "registrationDate": "1976-04-23",
                "endDate": "2005-12-18",
                "descriptions": [{"languageCode": "3", "description": "Registered"}],
            },
            {
                "type": "4",
                "register": "1",
                "registrationDate": "2005-12-19",
                "descriptions": [{"languageCode": "3", "description": "Ceased"}],
            },
        ],
    },
    # Missing businessId — must be dropped, never fabricated.
    {
        "businessId": {"value": None},
        "names": [{"name": "No Id Oy", "type": "1", "registrationDate": "2020-01-01"}],
        "addresses": [],
        "registeredEntries": [],
    },
]


def _write_fixture_zip(path: Path, companies: list[dict], member_name: str = "data_20260804.json") -> None:
    payload = json.dumps(companies).encode("utf-8")
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(member_name, payload)


def _fake_download_file_factory(zip_bytes_path: Path):
    def _fake_download_file(url, local_path, **kwargs):
        # Simulate a real download by copying the prebuilt fixture zip into place.
        local_path_obj = Path(local_path)
        local_path_obj.write_bytes(zip_bytes_path.read_bytes())
    return _fake_download_file


def test_finland_bulk_parsing_and_field_mapping(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_finland" in sys.modules:
        del sys.modules["update_finland"]
    mod = importlib.import_module("update_finland")

    fixture_zip = tmp_path / "fixture_all_companies.zip"
    _write_fixture_zip(fixture_zip, COMPANIES)

    monkeypatch.setattr(mod, "download_file", _fake_download_file_factory(fixture_zip))

    mod.main()

    out = tmp_path / "finland_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    # 3 companies in the fixture, 1 dropped for missing businessId -> 2 rows.
    assert df.height == 2

    active = df.filter(pl.col("registration_number") == "2283132-3").row(0, named=True)
    assert active["company_name"] == "Compater Oy"
    assert active["status"] == "Registered"
    assert active["registered_address"] == "Hyljetie 18 A, 02260, ESPOO"
    assert active["jurisdiction"] == "FI"
    assert active["ubo_names"] is None  # PRH's bulk file has no UBO data — never fabricate one

    ceased = df.filter(pl.col("registration_number") == "0100002-9").row(0, named=True)
    assert ceased["company_name"] == "Artjarven Metalli Oy"
    assert ceased["status"] == "Ceased"
    assert ceased["registered_address"] is None  # no addresses in the source for this company


def test_finland_exits_nonzero_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_finland" in sys.modules:
        del sys.modules["update_finland"]
    mod = importlib.import_module("update_finland")

    def _boom(url, local_path, **kwargs):
        raise mod.requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "download_file", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "finland_reg.parquet").exists()


def test_finland_exits_nonzero_on_malformed_zip(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_finland" in sys.modules:
        del sys.modules["update_finland"]
    mod = importlib.import_module("update_finland")

    def _fake_download_not_a_zip(url, local_path, **kwargs):
        Path(local_path).write_bytes(b"not actually a zip file")

    monkeypatch.setattr(mod, "download_file", _fake_download_not_a_zip)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "finland_reg.parquet").exists()


def test_finland_exits_nonzero_on_zero_valid_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_finland" in sys.modules:
        del sys.modules["update_finland"]
    mod = importlib.import_module("update_finland")

    fixture_zip = tmp_path / "fixture_empty.zip"
    # Only the no-businessId company -> zero valid rows after filtering.
    _write_fixture_zip(fixture_zip, [COMPANIES[2]])

    monkeypatch.setattr(mod, "download_file", _fake_download_file_factory(fixture_zip))

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "finland_reg.parquet").exists()
