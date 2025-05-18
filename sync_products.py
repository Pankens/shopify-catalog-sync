#!/usr/bin/env python3
import os
import json
import io
import time
import argparse
import math
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv

# ——— Carga de configuración ———
load_dotenv()
SHOP_URL       = os.getenv("SHOP_URL")      # e.g. "jq07uj-b8.myshopify.com"
SHOP_TOKEN     = os.getenv("SHOP_TOKEN")    # tu token privado con scopes read/write products
LOCATION_ID    = os.getenv("LOCATION_ID")   # gid://shopify/Location/...
RAW_SUBS       = os.getenv("SUBFAMILIAS", "")
PUBLICATION_ID = os.getenv("PUBLICATION_ID")# gid://shopify/Publication/...

# ——— Parámetros de cálculo de precio ———
IVA = 21.0  # % de IVA

# ——— Cabeceras y endpoint GraphQL ———
HEADERS          = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOP_TOKEN
}
API_VERSION      = "2024-10"
GRAPHQL_ENDPOINT = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"

SUBFAMILIAS = [s.strip() for s in RAW_SUBS.split(",") if s.strip()]


def fetch_external():
    productos = []
    for sub in SUBFAMILIAS:
        url = f"https://fastapi-megasur.onrender.com/catalogo?subfamilia={quote_plus(sub)}"
        resp = requests.get(url)
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

        # Cálculo de precio
        pvd_raw    = p.get("PVD", "0").replace(".", "").replace(",", ".")
        canon_raw  = p.get("CANON", "0").replace(".", "").replace(",", ".")
        margin_pct = float(p.get("MARGIN", "0").replace(".", "").replace(",", "."))
        base       = float(pvd_raw) + float(canon_raw)
        sin_iva    = base * (1 + margin_pct / 100)
        con_iva    = sin_iva * (1 + IVA / 100)
        precio     = math.floor(con_iva * 100) / 100.0

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
                    "price":             f"{precio:.2f}",
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
    vars = {"input": [{
        "resource":   "BULK_MUTATION_VARIABLES",
        "filename":   "productos.jsonl",
        "mimeType":   "text/jsonl",
        "httpMethod": "POST"
    }]}
    resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                         json={"query": mutation, "variables": vars})
    resp.raise_for_status()
    result = resp.json()["data"]["stagedUploadsCreate"]
    if result.get("userErrors"):
        print("❌ stagedUploadsCreate errors:")
        for err in result["userErrors"]:
            print("   •", err["field"], err["message"])
        raise SystemExit(1)
    tgt = result["stagedTargets"][0]
    params = {p["name"]: p["value"] for p in tgt["parameters"]}
    return tgt["url"], params, params["key"]


def upload_file(upload_url, upload_params, lines):
    content = "\n".join(json.dumps(l) for l in lines)
    files   = {"file": ("productos.jsonl", io.BytesIO(content.encode()), "application/json")}
    resp = requests.post(upload_url, data=upload_params, files=files)
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
    resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={
        "query":     mutation,
        "variables": {"stagedPath": staged_path, "productMutation": product_mutation}
    })
    resp.raise_for_status()
    data = resp.json()["data"]["bulkOperationRunMutation"]
    if data.get("userErrors"):
        print("❌ bulkOperationRunMutation errors:")
        for err in data["userErrors"]:
            print("   •", err["field"], err["message"])
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
        resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                             json={"query": query, "variables": {"id": bulk_id}})
        resp.raise_for_status()
        status = resp.json()["data"]["node"]["status"]
        print("⏳ BulkOperation status:", status)
        if status in ("COMPLETED", "FAILED", "CANCELED"):
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
        resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                             json={"query": query, "variables": {"cursor": cursor}})
        resp.raise_for_status()
        prod = resp.json()["data"]["products"]
        for edge in prod["edges"]:
            ids.append(edge["node"]["id"])
        if not prod["pageInfo"]["hasNextPage"]:
            break
        cursor = prod["edges"][-1]["cursor"]
    return ids


def delete_imported_products():
    """
    Elimina todos los productos que tengan la etiqueta 'ImportadoAPI'.
    """
    print("🧹 Eliminando productos previos con etiqueta ImportadoAPI…")
    ids = get_imported_ids()
    if not ids:
        print("   → No hay productos a eliminar.")
        return

    mutation = """
    mutation productDelete($input: ProductDeleteInput!) {
      productDelete(input: $input) {
        deletedProductId
        userErrors { field message }
      }
    }"""
    for pid in ids:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=HEADERS,
            json={"query": mutation, "variables": {"input": {"id": pid}}}
        )
        resp.raise_for_status()
        errs = resp.json().get("data", {}).get("productDelete", {}).get("userErrors", [])
        if errs:
            print(f"❌ Error borrando {pid}:", errs)
        else:
            print(f"🗑️  Producto borrado: {pid}")
    print(f"✅ Eliminados {len(ids)} productos previamente importados.")


def publish_to_online(ids):
    mutation = """
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }"""
    for pid in ids:
        variables = {"id": pid, "input": [{"publicationId": PUBLICATION_ID}]}
        resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                             json={"query": mutation, "variables": variables})
        resp.raise_for_status()
        data = resp.json()
        errs = data.get("data", {}).get("publishablePublish", {}).get("userErrors", [])
        if errs:
            print(f"❌ Errores publicando {pid}:", errs)
        else:
            print(f"✅ Publicado en Tienda Online: {pid}")


def main(dry_run=False):
    print("📦 Fetching externos…")
    externos = fetch_external()
    print(f"   → {len(externos)} productos descargados")
    if dry_run:
        return

    # ←───────── Aquí eliminamos todo lo anterior ───────────▶
    delete_imported_products()

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
