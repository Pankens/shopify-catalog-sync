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

SUBFAMILIAS = [s.strip() for s in RAW_SUBS.split(",") if s.strip()]


def fetch_external():
    productos = []
    for sub in SUBFAMILIAS:
        url = f"https://fastapi-megasur.onrender.com/catalogo?subfamilia={quote_plus(sub)}"
        resp = requests.get(url); resp.raise_for_status()
        productos.extend(resp.json())
    return productos


def get_imported_products_map():
    """
    Consulta todos los productos con tag ImportadoAPI y
    construye un dict sku -> productId
    """
    query = """
    query($cursor: String) {
      products(first: 100, after: $cursor, query: "tag:ImportadoAPI") {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            variants(first: 1) {
              edges { node { sku } }
            }
          }
        }
      }
    }"""
    existing = {}
    cursor = None
    while True:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=HEADERS,
            json={"query": query, "variables": {"cursor": cursor}}
        )
        resp.raise_for_status()
        data = resp.json()["data"]["products"]
        for edge in data["edges"]:
            node = edge["node"]
            var_edges = node["variants"]["edges"]
            if not var_edges:
                continue
            sku = var_edges[0]["node"]["sku"]
            existing[sku] = node["id"]
        if not data["pageInfo"]["hasNextPage"]:
            break
        cursor = data["pageInfo"]["endCursor"]
    return existing


def build_jsonl_lines(productos, existing_map):
    """
    Prepara las líneas JSONL para el Bulk:
     - deduplica en base a SKU
     - si sku en existing_map -> incluye "id" para hacer upsert
     - define productOptions, optionValues y files
    Retorna (lines, set_of_skus)
    """
    lines = []
    seen = set()
    new_skus = set()

    for p in productos:
        sku = str(p.get("REF", "")).strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        new_skus.add(sku)

        ean    = str(p.get("EAN", "")).strip()
        title  = p.get("NAME", "").strip()
        subfam = p.get("SUBFAMILIA", "").strip()
        desc   = p.get("DESCRIPTION", "").strip()
        img    = p.get("URL_IMG")
        stock  = int(float(p.get("STOCK", "0")))

        # cálculo de precio
        pvd_raw    = p.get("PVD", "0").replace(".", "").replace(",", ".")
        canon_raw  = p.get("CANON", "0").replace(".", "").replace(",", ".")
        margin_pct = float(p.get("MARGIN", "0").replace(".", "").replace(",", "."))
        base       = float(pvd_raw) + float(canon_raw)
        sin_iva    = base * (1 + margin_pct/100)
        con_iva    = sin_iva * (1 + IVA/100)
        price      = math.floor(con_iva * 100) / 100.0

        node = {
            # upsert si ya existía este SKU
            **({"id": existing_map[sku]} if sku in existing_map else {}),

            # estos tres bloques son obligatorios para ProductSetInput:
            "handle":          f"ean-{ean}",
            "title":           title,
            "descriptionHtml": desc,
            "status":          "ACTIVE",
            "productType":     subfam,
            "tags":            ["ImportadoAPI"],

            # 1) opciones de producto (necesarias para optionValues)
            "productOptions": [
                {"name": "SKU", "values": [{"name": sku}]}
            ],

            # 2) variante con su opción y stock
            "variants": [{
                "sku":             sku,
                "barcode":         ean,
                "price":           f"{price:.2f}",
                "inventoryPolicy": "DENY",
                "inventoryItem":   {"tracked": True},
                "inventoryQuantities": [
                    {"locationId": LOCATION_ID, "name": "available", "quantity": stock}
                ],
                # asociación variante→opción
                "optionValues": [
                    {"name": sku, "optionName": "SKU"}
                ]
            }]
        }

        # 3) imágenes con el nombre de campo correcto
        if img:
            node["files"] = [{"alt": title, "originalSource": img}]

        lines.append({"input": node})

    return lines, new_skus


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
    tgt = resp.json()["data"]["stagedUploadsCreate"]["stagedTargets"][0]
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
    errs = resp.json()["data"]["bulkOperationRunMutation"]["userErrors"]
    if errs:
        raise SystemExit(f"Errores bulkOperationRunMutation: {errs}")
    bulk_id = resp.json()["data"]["bulkOperationRunMutation"]["bulkOperation"]["id"]
    print("✅ Bulk iniciado:", bulk_id)
    return bulk_id


def wait_for_bulk(bulk_id, interval=5):
    query = """
    query($id: ID!) {
      node(id: $id) { ... on BulkOperation { status } }
    }"""
    while True:
        resp = requests.post(
            GRAPHQL_ENDPOINT, headers=HEADERS,
            json={"query": query, "variables": {"id": bulk_id}}
        )
        resp.raise_for_status()
        status = resp.json()["data"]["node"]["status"]
        print("⏳ BulkOperation status:", status)
        if status in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(interval)
    if status != "COMPLETED":
        raise RuntimeError(f"Bulk ended with status {status}")


def publish_to_online(ids):
    mutation = """
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors { field message }
      }
    }"""
    for pid in ids:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=HEADERS,
            json={"query": mutation, "variables": {"id": pid, "input": [{"publicationId": PUBLICATION_ID}]}}
        )
        resp.raise_for_status()
        errs = resp.json()["data"]["publishablePublish"]["userErrors"]
        if errs:
            print(f"❌ Error publicando {pid}:", errs)
        else:
            print(f"✅ Publicado: {pid}")


def delete_obsolete(existing_map, new_skus):
    obsolete = [pid for sku, pid in existing_map.items() if sku not in new_skus]
    if not obsolete:
        print("🗑️ No hay obsoletos que borrar.")
        return
    print(f"🗑️ Eliminando {len(obsolete)} obsoletos…")
    mutation = """
    mutation productDelete($input: ProductDeleteInput!) {
      productDelete(input: $input) {
        deletedProductId
        userErrors { field message }
      }
    }"""
    for pid in obsolete:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=HEADERS,
            json={"query": mutation, "variables": {"input": {"id": pid}}}
        )
        resp.raise_for_status()
        errs = resp.json()["data"]["productDelete"]["userErrors"]
        if errs:
            print(f"❌ Error borrando {pid}:", errs)
        else:
            print(f"🗑️ Borrado: {pid}")


def main(dry_run=False):
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Sólo fetch y conteo")
    args = parser.parse_args()

    print("📦 Fetching externos…")
    externos = fetch_external()
    print(f"   → {len(externos)} productos descargados")
    if args.dry_run:
        return

    # 1) Mapa sku->productId antes de actualizar
    existing_map = get_imported_products_map()

    # 2) Prepara JSONL con upsert por SKU
    print("🔨 Construyendo JSONL…")
    lines, new_skus = build_jsonl_lines(externos, existing_map)

    # 3) Staged upload y Bulk
    print("☁️ Creando staged upload…")
    upload_url, upload_params, staged_path = staged_upload()
    print("🚀 Subiendo archivo…")
    upload_file(upload_url, upload_params, lines)
    print("⏳ Ejecutando Bulk…")
    bulk_id = run_bulk(staged_path)
    print("⏳ Esperando fin…")
    wait_for_bulk(bulk_id)

    # 4) Publicar los productos actualizados/creados
    updated_map = get_imported_products_map()
    print("📢 Publicando…")
    publish_to_online(list(updated_map.values()))

    # 5) Borrar sólo los SKUs que ya no están
    delete_obsolete(existing_map, new_skus)


if __name__ == "__main__":
    main()
