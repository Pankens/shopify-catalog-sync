#!/usr/bin/env python3
import os
import json
import io
import time
import argparse
import math
from urllib.parse import quote_plus

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# ——— Carga de configuración ———
load_dotenv()
SHOP_URL       = os.getenv("SHOP_URL")
SHOP_TOKEN     = os.getenv("SHOP_TOKEN")
LOCATION_ID    = os.getenv("LOCATION_ID")
RAW_SUBS       = os.getenv("SUBFAMILIAS", "")
PUBLICATION_ID = os.getenv("PUBLICATION_ID")

# ——— Parámetros de cálculo de precio ———
IVA     = 21.0   # IVA en porcentaje
TIMEOUT = 30     # segundos para timeout en requests

# ——— Crear session con retries y headers ———
session = requests.Session()
session.headers.update({
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOP_TOKEN
})
retry = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[502, 503, 504],
    allowed_methods=frozenset(["GET", "POST"])
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ——— Endpoints y listas ———
GRAPHQL_ENDPOINT = f"https://{SHOP_URL}/admin/api/2024-10/graphql.json"
SUBFAMILIAS      = [s.strip() for s in RAW_SUBS.split(",") if s.strip()]


def fetch_external():
    productos = []
    for sub in SUBFAMILIAS:
        url = f"https://fastapi-megasur.onrender.com/catalogo?subfamilia={quote_plus(sub)}"
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        productos.extend(resp.json())
    return productos


def build_jsonl_lines(productos):
    lines = []
    for p in productos:
        ean    = str(p.get("EAN", ""))
        sku    = str(p.get("REF", ""))
        title  = p.get("NAME", "")
        subfam = p.get("SUBFAMILIA", "")

        # cálculo precio
        pvd_raw    = p.get("PVD", "0").replace(".", "").replace(",", ".")
        canon_raw  = p.get("CANON", "0").replace(".", "").replace(",", ".")
        margin_pct = float(p.get("MARGIN", "0").replace(".", "").replace(",", "."))
        pvd        = float(pvd_raw)
        canon      = float(canon_raw)
        base       = pvd + canon
        sin_iva    = base * (1 + margin_pct / 100)
        con_iva    = sin_iva * (1 + IVA / 100)
        precio_tr  = math.floor(con_iva * 100) / 100.0
        price      = f"{precio_tr:.2f}"

        stock  = int(float(p.get("STOCK", "0")))
        desc   = p.get("DESCRIPTION", "")
        img    = p.get("URL_IMG")
        handle = f"ean-{ean}"

        node = {
            "handle":          handle,
            "title":           title,
            "descriptionHtml": desc,
            "status":          "ACTIVE",
            "productType":     subfam,
            "tags":            ["ImportadoAPI"],
            "productOptions": [
                {"name": "SKU", "values": [{"name": sku}]}
            ],
            "variants": [
                {
                    "sku":               sku,
                    "barcode":           ean,
                    "price":             price,
                    "inventoryPolicy":   "DENY",
                    "inventoryItem":     {"tracked": True},
                    "inventoryQuantities": [
                        {"locationId": LOCATION_ID, "name": "available", "quantity": stock}
                    ],
                    "optionValues": [
                        {"name": sku, "optionName": "SKU"}
                    ]
                }
            ]
        }
        if img:
            node["files"] = [{"alt": title, "originalSource": img}]

        lines.append({"input": node})
    return lines


def staged_upload():
    mutation = """
    mutation($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets { url parameters { name value } }
        userErrors { field message }
      }
    }"""
    vars = {"input":[{
        "resource":   "BULK_MUTATION_VARIABLES",
        "filename":   "productos.jsonl",
        "mimeType":   "text/jsonl",
        "httpMethod": "POST"
    }]}
    resp = session.post(GRAPHQL_ENDPOINT,
                        json={"query":mutation, "variables":vars},
                        timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()["data"]["stagedUploadsCreate"]
    if data.get("userErrors"):
        print("❌ stagedUploadsCreate errors:", data["userErrors"])
        raise SystemExit(1)
    tgt    = data["stagedTargets"][0]
    params = {p["name"]: p["value"] for p in tgt["parameters"]}
    return tgt["url"], params, params["key"]


def upload_file(upload_url, upload_params, lines):
    content = "\n".join(json.dumps(l) for l in lines)
    files   = {"file": ("productos.jsonl", io.BytesIO(content.encode()), "application/json")}
    resp = session.post(upload_url, data=upload_params, files=files, timeout=TIMEOUT)
    resp.raise_for_status()


def run_bulk(staged_path):
    mutation = """
    mutation($stagedPath: String!, $productMutation: String!) {
      bulkOperationRunMutation(
        mutation: $productMutation,
        stagedUploadPath: $stagedPath
      ) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }"""
    product_mutation = """
    mutation productUpsert($input: ProductSetInput!) {
      productSet(input: $input) { product { id } userErrors { field message } }
    }"""
    resp = session.post(GRAPHQL_ENDPOINT,
                        json={"query":mutation,
                              "variables":{"stagedPath":staged_path,
                                           "productMutation":product_mutation}},
                        timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()["data"]["bulkOperationRunMutation"]
    if data.get("userErrors"):
        print("❌ bulkOperationRunMutation errors:", data["userErrors"])
        raise SystemExit(1)
    bulk_id = data["bulkOperation"]["id"]
    print("✅ Bulk iniciado:", bulk_id)
    return bulk_id


def wait_for_bulk(bulk_id, interval=5):
    query = """
    query($id: ID!) {
      node(id: $id) {
        ... on BulkOperation { status }
      }
    }"""
    while True:
        resp = session.post(GRAPHQL_ENDPOINT,
                            json={"query":query, "variables":{"id":bulk_id}},
                            timeout=TIMEOUT)
        resp.raise_for_status()
        status = resp.json()["data"]["node"]["status"]
        print("⏳ BulkOperation status:", status)
        if status in ("COMPLETED","FAILED","CANCELED"):
            break
        time.sleep(interval)
    if status != "COMPLETED":
        raise RuntimeError(f"Bulk ended with status {status}")


def get_imported_ids():
    query = """
    query($cursor: String) {
      products(first: 100, after: $cursor, query: "tag:ImportadoAPI") {
        pageInfo { hasNextPage }
        edges { cursor node { id } }
      }
    }"""
    ids, cursor = [], None
    while True:
        resp = session.post(GRAPHQL_ENDPOINT,
                            json={"query":query,"variables":{"cursor":cursor}},
                            timeout=TIMEOUT)
        resp.raise_for_status()
        prod = resp.json()["data"]["products"]
        for edge in prod["edges"]:
            ids.append(edge["node"]["id"])
        if not prod["pageInfo"]["hasNextPage"]:
            break
        cursor = prod["edges"][-1]["cursor"]
    return ids


def delete_previous_imports():
    ids = get_imported_ids()
    if not ids:
        return
    print(f"🗑️ Borrando {len(ids)} productos importados anteriormente…")
    mutation = """
    mutation deleteProduct($id: ID!) {
      productDelete(id: $id) {
        deletedProductId
        userErrors { field message }
      }
    }"""
    for pid in ids:
        resp = session.post(GRAPHQL_ENDPOINT,
                            json={"query":mutation,"variables":{"id":pid}},
                            timeout=TIMEOUT)
        data = resp.json()
        errs = data.get("data",{}).get("productDelete",{}).get("userErrors",[])
        if errs:
            print("  ❌ No pude borrar", pid, errs)
    print("🗑️ Borrado completado.")


def publish_to_online(ids):
    mutation = """
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { message }
      }
    }"""
    success = 0
    for pid in ids:
        resp = session.post(GRAPHQL_ENDPOINT,
                            json={"query":mutation,
                                  "variables":{"id":pid,
                                               "input":[{"publicationId":PUBLICATION_ID}]}},
                            timeout=TIMEOUT)
        data = resp.json()
        if not data.get("errors") and not data["data"]["publishablePublish"]["userErrors"]:
            success += 1
    print(f"✅ Publicados {success}/{len(ids)} productos en Tienda Online.")


def main(dry_run=False):
    print("🚀 Limpiando importaciones anteriores…")
    delete_previous_imports()

    print("📦 Fetching externos…")
    externos = fetch_external()
    print(f"   → {len(externos)} productos descargados")
    if dry_run:
        return

    print("🔨 Construyendo JSONL…")
    lines = build_jsonl_lines(externos)

    print("☁️ Creando staged upload…")
    upload_url, upload_params, staged_path = staged_upload()

    print("🚀 Subiendo archivo…")
    upload_file(upload_url, upload_params, lines)

    print("⏳ Ejecutando Bulk…")
    bulk_id = run_bulk(staged_path)

    print("⏳ Esperando a que termine el Bulk…")
    wait_for_bulk(bulk_id)

    print("📢 Publicando en Tienda Online…")
    ids = get_imported_ids()
    publish_to_online(ids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo descarga y cuenta productos")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
