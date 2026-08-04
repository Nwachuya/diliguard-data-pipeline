"""Real ingestion of Spain's BORME (Boletín Oficial del Registro Mercantil) — no auth
required.

Source: https://www.boe.es/datosabiertos/api/borme/sumario/{YYYYMMDD} (AEBOE, OpenAPI
3.1.0, documented at https://www.boe.es/datosabiertos/documentos/APIsumarioBORME.pdf).
BORME is Spain's official daily companies-registry gazette: every business day it
publishes "Sección A" (Empresarios. Actos inscritos) — one item per province, each
item containing free-text legal notices for every company act (constitución, ceses,
nombramientos, cambios de capital, disolución, etc.) registered that day.

Investigated and rejected as an alternative: https://openmercantil.es/ has nicer
per-company pages (and schema.org JSON-LD with company_name + CIF), but there is no
bulk API or CSV — only ~2.8M individual HTML pages discoverable via sitemap, and its
own robots.txt / page content show `/export` and `/checkout-success` gated behind a
paid Stripe flow. That's not an instant self-serve bulk source, so this script uses
the official BOE API directly instead.

Important, deliberate schema limitations of this source (not gaps we're hiding):
- There is no CIF/NIF or clean "registration number" field in BORME text. The closest
  real identifier is the Registro Mercantil sheet reference from "Datos registrales"
  (e.g. "H A 18723" = Hoja A-18723 in the Alicante mercantile registry). We use that,
  verbatim, as registration_number.
- registered_address is only present in the text for acts that state a "Domicilio:"
  (overwhelmingly "Constitución" = new-company acts). For all other act types
  (nombramientos, ceses, ampliación de capital, ...) it is left None — BORME's daily
  bulletin genuinely does not repeat the registered address on every act.
- status is only inferable from the act type itself: "Constitución" -> ACTIVE (newly
  registered), "Disolución"/"Extinción" -> DISSOLVED. Any other act type (a director
  appointment, a capital change, etc.) tells us nothing about current company status,
  so it is left None rather than guessed.
- ubo_names is always None. BORME publishes administrators/directors ("Adm. Unico",
  "Consejero", "Apo.Manc.", ...), which are NOT beneficial owners in the AML/UBO
  sense, and BORME does not publish beneficial-ownership data at all. Mapping
  directors into ubo_names would misrepresent what the field means, so it is
  deliberately left null rather than being filled with the wrong kind of person.

BORME is only published on Spanish business days, so this script walks backward from
today (up to BACKTRACK_DAYS) looking for the most recent date with a real sumario. A
404 for a given date genuinely means "no bulletin that day" (weekend/holiday) per
AEBOE's own API and is not treated as a failure. Any other fetch/parse failure is
fatal: this script exits non-zero and writes nothing.
"""
import html
import os
import re
import sys
from datetime import datetime, timedelta, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common.http import get_with_retry
from common.parquet_io import write_registry_parquet

SUMARIO_URL_TEMPLATE = "https://www.boe.es/datosabiertos/api/borme/sumario/{date}"
BACKTRACK_DAYS = 10
SECCION_EMPRESARIOS_CODE = "A"

ARTICULO_PARRAFO_RE = re.compile(
    r'<p class="articulo">(.*?)</p>\s*<p class="parrafo">(.*?)</p>', re.S
)
NAME_RE = re.compile(r'\s*\d+\s*-\s*(.*?)\.?\s*$')
HOJA_RE = re.compile(r'H\s*([A-Z]{1,2}\s*\d+)')
DOMICILIO_RE = re.compile(
    r'Domicilio:\s*(.*?)\.\s*(?:Capital:|Declaraci|Nombramientos|Ceses|Datos registrales|$)'
)


def _find_latest_sumario() -> tuple[str, dict]:
    """Walk backward from today looking for the most recent published sumario.

    A 404 means "AEBOE has no bulletin for this date" (weekend/holiday) and is not
    an error — we just try the previous day. Any other HTTP/network failure is
    fatal and propagates up.
    """
    today = datetime.now(timezone.utc).date()
    for offset in range(BACKTRACK_DAYS + 1):
        date_str = (today - timedelta(days=offset)).strftime("%Y%m%d")
        url = SUMARIO_URL_TEMPLATE.format(date=date_str)
        try:
            response = get_with_retry(url, timeout=30, headers={"Accept": "application/json"})
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue
            raise
        return date_str, response.json()
    raise RuntimeError(f"no BORME sumario found in the last {BACKTRACK_DAYS} days")


def _section_a_item_xml_urls(sumario_payload: dict) -> list[str]:
    diarios = sumario_payload["data"]["sumario"]["diario"]
    if isinstance(diarios, dict):
        diarios = [diarios]
    urls = []
    for diario in diarios:
        secciones = diario.get("seccion") or []
        if isinstance(secciones, dict):
            secciones = [secciones]
        for seccion in secciones:
            if seccion.get("codigo") != SECCION_EMPRESARIOS_CODE:
                continue
            items = seccion.get("item") or []
            if isinstance(items, dict):
                items = [items]
            for item in items:
                url_xml = item.get("url_xml")
                if url_xml:
                    urls.append(url_xml)
    return urls


def _infer_status(body: str) -> str | None:
    if "Extinci" in body or "Disoluci" in body:
        return "DISSOLVED"
    if "Constituci" in body:
        return "ACTIVE"
    return None


def _rows_from_item_xml(xml_text: str) -> list[dict]:
    rows = []
    for raw_name, raw_body in ARTICULO_PARRAFO_RE.findall(xml_text):
        name = html.unescape(raw_name)
        body = html.unescape(raw_body)

        m_name = NAME_RE.match(name)
        company_name = m_name.group(1).strip() if m_name else name.strip()
        if not company_name:
            continue

        m_hoja = HOJA_RE.search(body)
        registration_number = f"H {m_hoja.group(1)}".strip() if m_hoja else None

        m_addr = DOMICILIO_RE.search(body)
        registered_address = m_addr.group(1).strip() if m_addr else None

        rows.append({
            "company_name": company_name,
            "registration_number": registration_number,
            "registered_address": registered_address,
            "status": _infer_status(body),
            "ubo_names": None,  # BORME publishes directors/administrators, not beneficial owners
            "jurisdiction": "ES",
        })
    return rows


def main():
    print("Looking for the most recent published BORME sumario...")
    try:
        date_str, sumario_payload = _find_latest_sumario()
    except (requests.RequestException, ValueError, KeyError, RuntimeError) as e:
        print(f"FATAL: failed to fetch a BORME sumario: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Using BORME sumario for {date_str}")

    try:
        item_urls = _section_a_item_xml_urls(sumario_payload)
    except (KeyError, TypeError) as e:
        print(f"FATAL: unexpected BORME sumario shape: {e}", file=sys.stderr)
        sys.exit(1)

    if not item_urls:
        print(f"FATAL: BORME sumario for {date_str} had zero Sección A items — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    rows: list[dict] = []
    try:
        for url in item_urls:
            response = get_with_retry(url, timeout=30, headers={"Accept": "application/xml"})
            rows.extend(_rows_from_item_xml(response.text))
    except requests.RequestException as e:
        print(f"FATAL: failed to fetch a BORME item document: {e}", file=sys.stderr)
        sys.exit(1)

    if not rows:
        print(f"FATAL: parsed zero company rows from BORME sumario {date_str} — refusing to write an empty file", file=sys.stderr)
        sys.exit(1)

    source_url = SUMARIO_URL_TEMPLATE.format(date=date_str)
    count = write_registry_parquet(rows, "spain_reg.parquet", source_url=source_url)
    print(f"Wrote {count} real Spain BORME rows (sumario {date_str}, {len(item_urls)} items) to spain_reg.parquet")


if __name__ == "__main__":
    main()
