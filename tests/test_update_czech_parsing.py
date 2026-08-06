"""Fixture-based test of the real Czech ARES bulk-export parsing logic (no network).

The fixture XML member bodies below are trimmed real shapes confirmed live by
downloading and inspecting actual members of https://ares.gov.cz/otevrena-data/
ares_vreo_all.tar.gz during development of scripts/update_czech.py (e.g. IČO
00000108 "Závodní klub OS KOVO Buzuluk Komárov" and IČO 00000124 "INSTITUT
ŘÍZENÍ", the latter showing the real DatumVymazu-present/TERMINATED shape).
"""
import importlib
import io
import sys
import tarfile
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

NS = "http://wwwinfo.mfcr.cz/ares/xml_doc/schemas/ares/ares_answer_vreo/v_1.0.0"


def _member_xml(ico: str, firma: str, address: str, *, datum_vymazu: str | None = None) -> bytes:
    vymaz = f"<are:DatumVymazu>{datum_vymazu}</are:DatumVymazu>" if datum_vymazu else ""
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<are:Ares_odpovedi xmlns:are="{NS}" odpoved_datum_cas="2026-04-23T11:11:57" '
        'odpoved_pocet="1" odpoved_typ="Vypis_VREO" validation_XSLT="x" Id="aresds">'
        "<are:Odpoved><are:Pomocne_ID>0</are:Pomocne_ID>"
        "<are:Vysledek_hledani><are:Kod>1</are:Kod></are:Vysledek_hledani>"
        "<are:Pocet_zaznamu>1</are:Pocet_zaznamu>"
        "<are:Vypis_VREO><are:Uvod><are:Nadpis>Výpis</are:Nadpis></are:Uvod>"
        "<are:Zakladni_udaje>"
        "<are:Rejstrik>OR</are:Rejstrik>"
        f"<are:ICO>{ico}</are:ICO>"
        f"<are:ObchodniFirma>{firma}</are:ObchodniFirma>"
        f"<are:Sidlo><are:text>{address}</are:text></are:Sidlo>"
        "<are:DatumZapisu>1990-02-27</are:DatumZapisu>"
        f"{vymaz}"
        "</are:Zakladni_udaje></are:Vypis_VREO></are:Odpoved></are:Ares_odpovedi>"
    )
    return xml.encode("utf-8")


def _build_fixture_archive(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, mode="w:gz") as tf:
        for name, data in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def _fresh_module():
    if "update_czech" in sys.modules:
        del sys.modules["update_czech"]
    return importlib.import_module("update_czech")


def test_czech_bulk_archive_parsing_and_status(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    archive = tmp_path / "fixture.tar.gz"
    _build_fixture_archive(archive, {
        "./VYSTUP/DATA/00000108.xml": _member_xml(
            "00000108", "Závodní klub OS KOVO Buzuluk Komárov", "Buzulucká 440, 26762 Komárov",
        ),
        "./VYSTUP/DATA/00000124.xml": _member_xml(
            "00000124", "INSTITUT ŘÍZENÍ", "Jungmannova 29, 11000 Praha 1, Česká republika",
            datum_vymazu="1994-08-01",
        ),
    })

    def _fake_download(url, local_path, **kwargs):
        # Simulate the real download by copying our fixture archive into place.
        local_path_obj = tmp_path / local_path if not str(local_path).startswith("/") else Path(local_path)
        local_path_obj.write_bytes(archive.read_bytes())

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "czech_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 2

    active = df.filter(pl.col("registration_number") == "00000108").row(0, named=True)
    assert active["company_name"] == "Závodní klub OS KOVO Buzuluk Komárov"
    assert active["registered_address"] == "Buzulucká 440, 26762 Komárov"
    assert active["status"] == "ACTIVE"
    assert active["ubo_names"] is None  # real gap — Statutarni_organ is not UBO data
    assert active["jurisdiction"] == "CZ"

    terminated = df.filter(pl.col("registration_number") == "00000124").row(0, named=True)
    assert terminated["status"] == "TERMINATED"  # driven by real DatumVymazu presence


def test_czech_exits_nonzero_on_download_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    def _boom(url, local_path, **kwargs):
        raise RuntimeError("simulated network failure")

    monkeypatch.setattr(mod, "download_file", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "czech_reg.parquet").exists()


def test_czech_exits_nonzero_on_zero_rows(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    archive = tmp_path / "empty_fixture.tar.gz"
    _build_fixture_archive(archive, {})

    def _fake_download(url, local_path, **kwargs):
        local_path_obj = tmp_path / local_path if not str(local_path).startswith("/") else Path(local_path)
        local_path_obj.write_bytes(archive.read_bytes())

    monkeypatch.setattr(mod, "download_file", _fake_download)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "czech_reg.parquet").exists()


def test_czech_skips_unparseable_member_without_fabricating(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    mod = _fresh_module()

    archive = tmp_path / "fixture_with_junk.tar.gz"
    _build_fixture_archive(archive, {
        "./VYSTUP/DATA/00000108.xml": _member_xml(
            "00000108", "Závodní klub OS KOVO Buzuluk Komárov", "Buzulucká 440, 26762 Komárov",
        ),
        "./VYSTUP/DATA/broken.xml": b"<not well formed xml",
    })

    def _fake_download(url, local_path, **kwargs):
        local_path_obj = tmp_path / local_path if not str(local_path).startswith("/") else Path(local_path)
        local_path_obj.write_bytes(archive.read_bytes())

    monkeypatch.setattr(mod, "download_file", _fake_download)

    mod.main()

    out = tmp_path / "czech_reg.parquet"
    df = pl.read_parquet(out)
    assert df.height == 1  # the one real, well-formed row — junk member skipped, not fabricated
