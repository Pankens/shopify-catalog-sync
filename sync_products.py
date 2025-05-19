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
SHOP_TOKEN     = os.getenv("SHOP_TOKEN")    # token privado con scopes read_products, write_products
LOCATION_ID    = os.getenv("LOCATION_ID")   # gid://shopify/Location/{ID}
RAW_SUBS       = os.getenv("SUBFAMILIAS", "")
PUBLICATION_ID = os.getenv("PUBLICATION_ID")# gid://shopify/Publication/{ID}

# ——— Parámetros de cálculo de precio ———
IVA = 21.0  # IVA en porcentaje

# ——— Cabeceras y endpoint GraphQL ———
API_VERSION      = "2024-10"
GRAPHQL_ENDPOINT = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOP_TOKEN
}

# Lista de subfamilias a consultar
SUBFAMILIAS = [s.strip() for s in RAW_SUBS.split(",") if s.strip()]


def fetch_external():
    """
    Llama a la API externa por cada subfamilia y recopila todos los productos.
    """
    productos = []
    for sub in SUBFAMILIAS:
        url = f"https://fastapi-megasur.onrender.com/catalogo?subfamilia={quote_plus(sub)}"
        resp = requests.get(url)
        resp.raise_for_status()
        productos.extend(resp.json())
    return productos


def get_imported_products_map():
    """
    Obtiene todos los productos ya importados (etiqueta ImportadoAPI) y devuelve
    un dict mapping handle -> product ID.
    """
    query = '''
    query($cursor: String) {
      products(first: 100, after: $cursor, query: "tag:ImportadoAPI") {
        pageInfo { hasNextPage endCursor }
        edges { cursor node { id handle } }
      }
    }'''
    existing = {}
    cursor = None
    while True:
        variables = {"cursor": cursor}
        resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                             json={"query": query, "variables": variables})
        resp.raise_for_status()
        data = resp.json()["data"]["products"]
        for edge in data["edges"]:
            node = edge["node"]
            existing[node["handle"]] = node["id"]
        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["pageInfo"]["endCursor"]
    return existing


def build_jsonl_lines(productos):
    """
    Construye las líneas JSONL para Bulk API y devuelve (líneas, set(handles)).
    """
    lines = []
    handles = set()
    for p in productos:
        ean    = str(p.get("EAN", ""))
        sku    = str(p.get("REF", ""))
        title  = p.get("NAME", "")
        subfam = p.get("SUBFAMILIA", "")

        # Cálculo de precio con IVA y margen
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
        handles.add(handle)

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
    return lines, handles


def staged_upload():
    mutation = '''
    mutation($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets { url parameters { name value } }
        userErrors { field message }
      }
    }'''
    variables = {"input": [{
        "resource":   "BULK_MUTATION_VARIABLES",
        "filename":   "productos.jsonl",
        "mimeType":   "text/jsonl",
        "httpMethod": "POST"
    }]}
    resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS,
                         json={"query": mutation, "variables": variables})
    resp.raise_for_status()
    result = resp.json()["data"]["stagedUploadsCreate"]
    if result.get("userErrors"):
        raise SystemExit("Errores en stagedUploadsCreate: %s" % result["userErrors"])
    target = result["stagedTargets"][0]
    params = {p["name"]: p["value"] for p in target["parameters"]}
    return target["url"], params, params["key"]


def upload_file(upload_url, upload_params, lines):
    content = "\n".join(json.dumps(l) for l in lines)
    files   = {"file": ("productos.jsonl", io.BytesIO(content.encode()), "application/json")}
    resp = requests.post(upload_url, data=upload_params, files=files)
    resp.raise_for_status()


def run_bulk(staged_path):
    mutation = '''
    mutation($stagedPath: String!, $productMutation: String!) {
      bulkOperationRunMutation(
        mutation: $productMutation,
        stagedUploadPath: $stagedPath
      ) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }'''
    product_mutation = '''
    mutation productUpsert($input: ProductSetInput!) {
      productSet(input: $input) { product { id } userErrors { field message } }
    }'''
    resp = requests.post(GRAPHQL_ENDPOINT, headers=HEADERS, json={
        "query":     mutation,
        "variables": {"stagedPath": staged_path, "productMutation": product_mutation}
    })
    resp.raise_for_status()
    data = resp.json()["data"]["bulkOperationRunMutation"]
    if data.get("userErrors"):
        raise SystemExit("Errores en bulkOperationRunMutation: %s" % data["userErrors"])
    bulk_id = data["bulkOperation"]["id"]
    print("✅ Bulk iniciado:", bulk_id)
    return bulk_id


def wait_for_bulk(bulk_id, interval=5):
    query = '''
    query($id: ID!) {
      node(id: $id) { ... on BulkOperation { status } }
    }'''
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


def publish_to_online(ids):
    mutation = '''
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }'''
    for pid in ids:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=HEADERS,
            json={"query": mutation, "variables": {"id": pid, "input": [{"publicationId": PUBLICATION_ID}]}}
        )
        resp.raise_for_status()
        errs = resp.json().get("data", {}).get("publishablePublish", {}).get("userErrors", [])
        if errs:
            print(f"❌ Error publicando {pid}:", errs)
        else:
            print(f"✅ Publicado en Tienda Online: {pid}")


def delete_obsolete(existing_map, new_handles):
    """
    Borra (productDelete) aquellos productos importados previamente cuya handle
    ya no aparece en new_handles.
    """
    obsolete = [pid for h, pid in existing_map.items() if h not in new_handles]
    if not obsolete:
        print("🗑️  No hay productos obsoletos para eliminar.")
        return

    print(f"🗑️  Eliminando {len(obsolete)} productos obsoletos…")
    mutation = '''
    mutation productDelete($input: ProductDeleteInput!) {
      productDelete(input: $input) { deletedProductId userErrors { field message } }
    }'''
    for pid in obsolete:
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
            print(f"🗑️  Borrado producto: {pid}")


def main(dry_run=False):
    print("📦 Fetching externos…")
    externos = fetch_external()
    print(f"   → {len(externos)} productos descargados")
    if dry_run:
        return

    # 1) Map de productos ya importados
    existing_map = get_imported_products_map()

    # 2) Construir JSONL y obtener handles nuevos
    print("🔨 Construyendo JSONL…")
    lines, new_handles = build_jsonl_lines(externos)

    # 3) Subida staged & Bulk import
    print("☁️ Creando staged upload…")
    upload_url, upload_params, staged_path = staged_upload()

    print("🚀 Subiendo archivo…")
    upload_file(upload_url, upload_params, lines)

    print("⏳ Ejecutando Bulk…")
    bulk_id = run_bulk(staged_path)

    print("⏳ Esperando a que termine el Bulk…")
    wait_for_bulk(bulk_id)

    # 4) Publicar todos los importados en el canal online
    ids = list(existing_map.values())  # publishablePublish no requiere acotar solo nuevos
    print("📢 Publicando en Tienda Online…")
    publish_to_online(ids)

    # 5) Eliminar los obsoletos
    delete_obsolete(existing_map, new_handles)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo descarga y cuenta productos")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
