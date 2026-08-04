"""Fixture-based test of the real Spain BORME sumario/item parsing logic (no network)."""
import importlib
import sys
from pathlib import Path

import polars as pl
import pytest

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SUMARIO_FIXTURE = {
    "status": {"code": "200", "text": "ok"},
    "data": {
        "sumario": {
            "metadatos": {"publicacion": "BORME", "fecha_publicacion": "20260804"},
            "diario": [
                {
                    "numero": "148",
                    "seccion": [
                        {
                            "codigo": "A",
                            "nombre": "SECCIÓN PRIMERA. Empresarios. Actos inscritos",
                            "item": [
                                {
                                    "identificador": "BORME-A-2026-148-01",
                                    "titulo": "TEST PROVINCE",
                                    "url_xml": "https://www.boe.es/diario_borme/xml.php?id=BORME-A-2026-148-01",
                                }
                            ],
                        },
                        {
                            "codigo": "C",
                            "nombre": "SECCIÓN SEGUNDA. Anuncios y avisos legales",
                            "item": [],
                        },
                    ],
                }
            ],
        }
    },
}

ITEM_XML_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<documento>
  <texto>
    <p class="articulo">360082 - TEST FIXTURE CONSTITUCION SL.</p>
    <p class="parrafo">Constitución. Comienzo de operaciones: 1.08.26. Objeto social: Pruebas. Domicilio: C/ FALSA 123 (MADRID). Capital: 3.000,00 Euros. Datos registrales. S 8 , H M 999999, I/A 1 (01.08.26).</p>
  </texto>
  <texto>
    <p class="articulo">360083 - TEST FIXTURE CESE SL.</p>
    <p class="parrafo">Ceses/Dimisiones. Consejero: FULANO DE TAL.  Datos registrales. S 8 , H M 888888, I/A 5 (01.08.26).</p>
  </texto>
  <texto>
    <p class="articulo">360084 - TEST FIXTURE DISOLUCION SL.</p>
    <p class="parrafo">Disolución. Extinción.  Datos registrales. S 8 , H M 777777, I/A 9 (01.08.26).</p>
  </texto>
</documento>"""


class _FakeJsonResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeXmlResponse:
    def __init__(self, text):
        self.text = text


def test_spain_borme_parsing(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_spain" in sys.modules:
        del sys.modules["update_spain"]
    mod = importlib.import_module("update_spain")

    calls = {"n": 0}

    def _fake_get(url, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeJsonResponse(SUMARIO_FIXTURE)
        return _FakeXmlResponse(ITEM_XML_FIXTURE)

    monkeypatch.setattr(mod, "get_with_retry", _fake_get)

    mod.main()

    out = tmp_path / "spain_reg.parquet"
    assert out.exists()
    df = pl.read_parquet(out).sort("registration_number")
    assert df.height == 3

    constitucion = df.filter(pl.col("registration_number") == "H M 999999").row(0, named=True)
    assert constitucion["company_name"] == "TEST FIXTURE CONSTITUCION SL"
    assert constitucion["status"] == "ACTIVE"
    assert constitucion["registered_address"] == "C/ FALSA 123 (MADRID)"
    assert constitucion["ubo_names"] is None  # BORME has no beneficial-owner data
    assert constitucion["jurisdiction"] == "ES"

    cese = df.filter(pl.col("registration_number") == "H M 888888").row(0, named=True)
    assert cese["company_name"] == "TEST FIXTURE CESE SL"
    assert cese["status"] is None  # a director cessation tells us nothing about company status
    assert cese["registered_address"] is None  # not restated in this act type

    disolucion = df.filter(pl.col("registration_number") == "H M 777777").row(0, named=True)
    assert disolucion["status"] == "DISSOLVED"


def test_spain_exits_nonzero_on_request_failure(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    if "update_spain" in sys.modules:
        del sys.modules["update_spain"]
    mod = importlib.import_module("update_spain")

    def _boom(url, **kwargs):
        raise mod.requests.RequestException("simulated failure")

    monkeypatch.setattr(mod, "get_with_retry", _boom)

    with pytest.raises(SystemExit) as exc_info:
        mod.main()
    assert exc_info.value.code == 1
    assert not (tmp_path / "spain_reg.parquet").exists()
