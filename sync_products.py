#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_products.py
Sincroniza desde https://fastapi-megasur.onrender.com/catalogo?subfamilia=… hacia Shopify:
- Borra todo lo importado con tag ImportadoAPI
- Sube en bulk el nuevo catálogo (sin duplicados)
- Publica al canal Online Store
"""

import os
import json
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# ——— Carga .env en local (opcional en GH Actions) ———
load_dotenv()

# ——— Variables de entorno existentes ———
SHOP_URL       = os.getenv("SHOP_URL")       # tu-tienda.myshopify.com
SHOP_TOKEN     = os.getenv("SHOP_TOKEN")     # shpat_...
SUBFAMILIAS    = [s.strip() for s in os.getenv("SUBFAMILIAS", "").split(",") if s.strip()]
LOCATION_ID    = os.getenv("LOCATION_ID")    # gid://shopify/Location/...
PUBLICATION_ID = os.getenv("PUBLICATION_ID") # gid://shopify/Publication/...
CHANNEL_ID     = os.getenv("CHANNEL_ID")     # gid://shopify/Channel/...

# Validaciones mínimas
if not SHOP_URL or not SHOP_TOKEN:
    raise EnvironmentError("❌ Debes definir SHOP_URL y SHOP_TOKEN en las env vars")
if not SUBFAMILIAS:
    raise EnvironmentError("❌ Debes definir SUBFAMILIAS en las env vars")
if not LOCATION_ID:
    raise EnvironmentError("❌ Debes definir LOCATION_ID en las env vars")
if not PUBLICATION_ID or not CHANNEL_ID:
    raise EnvironmentError("❌ Debes definir PUBLICATION_ID y CHANNEL_ID en las env vars")

# Parámetros
API_VERSION = "2024-10"
GRAPHQL_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
REST_BASE   = f"https://{SHOP_URL}/admin/api/{API_VERSION}"

IMPORT_TAG  = "ImportadoAPI"
TIMEOUT     = 30
MAX_RETRIES = 3
BACKOFF     = 5  # segundos base

# Sesión con retry adapter para _fallos de red_ muy groseros
session = requests.Session()
retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=1,
    status_forcelist=[502,503,504],
    allowed_methods=["GET","POST","DELETE"]
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
session.mount("http://", HTTPAdapter(max_retries=retry_strategy))
session.headers.update({
    "X-Shopify-Access-Token": SHOP_TOKEN,
    "Content-Type": "application/json"
})


def request_with_retry(method, url, **kwargs):
    """
    Agrega reintentos adicionales y backoff para códigos 429, 500+, y errores de conexión.
    """
    for attempt in range(1, MAX_RETRIES+1):
        try:
            resp = session.request(method, url, timeout=TIMEOUT, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES:
                print(f"[WARN] Conexión falló (intento {attempt}): {e}, reintento en {BACKOFF*attempt}s")
                time.sleep(BACKOFF*attempt)
                continue
            print(f"[ERROR] Conexión definitivamente fallida: {e}")
            return None
        if resp.status_code in (429,) or resp.status_code >= 500:
            if attempt < MAX_RETRIES:
                print(f"[WARN] HTTP {resp.status_code} en {url} (intento {attempt}), reintento en {BACKOFF*attempt}s")
                time.sleep(BACKOFF*attempt)
                continue
        return resp
    return None


def fetch_external():
    """Descarga todas las SUBFAMILIAS desde tu FastAPI y devuelve lista unificada sin duplicados."""
    URL_TEMPLATE = "https://fastapi-megasur.onrender.com/catalogo?subfamilia={}"
    productos = []
    # Peticiones en paralelo
    def fetch(sub):
        url = URL_TEMPLATE.format(requests.utils.requote_uri(sub))
        r = request_with_retry("GET", url)
        if not r or r.status_code != 200:
            print(f"[ERROR] Falló descarga subfamilia {sub}: HTTP {r.status_code if r else 'NoResp'}")
            return []
        try:
            return r.json()
        except:
            print(f"[ERROR] JSON inválido en subfamilia {sub}")
            return []

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch, s): s for s in SUBFAMILIAS}
        for f in as_completed(futures):
            productos.extend(f.result())

    # Eliminar duplicados por SKU/EAN
    seen = set()
    unique = []
    for p in productos:
        key = str(p.get("REF","") or p.get("EAN","") or p.get("SKU","")).strip()
        if not key: 
            continue
        if key in seen: 
            continue
        seen.add(key)
        unique.append(p)
    print(f"→ {len(unique)} productos únicos obtenidos")
    return unique


def run_bulk_query_to_delete():
    """BulkQuery GraphQL que devuelve URL con todos los GIDs de los productos tagueados."""
    QUERY = f'''
    mutation {{
      bulkOperationRunQuery(
        query: """
          {{ products(query: "tag:{IMPORT_TAG}") {{ edges {{ node {{ id }} }} }} }}
        """
      ) {{
        bulkOperation {{ id status }}
        userErrors {{ message field }}
      }}
    }}
    '''
    resp = request_with_retry("POST", GRAPHQL_URL, json={"query": QUERY})
    if not resp: raise RuntimeError("No arrancó bulkOperationRunQuery")
    d = resp.json()
    errs = d["data"]["bulkOperationRunQuery"]["userErrors"]
    if errs:
        raise RuntimeError(f"bulkOperationRunQuery userErrors: {errs}")
    bulk_id = d["data"]["bulkOperationRunQuery"]["bulkOperation"]["id"]
    print(f"BulkQuery iniciado (ID={bulk_id}), esperando resultado…")

    # Esperar a COMPLETED y sacar la URL de datos
    while True:
        time.sleep(2)
        status_q = "query { currentBulkOperation { status url }} "
        r2 = request_with_retry("POST", GRAPHQL_URL, json={"query": status_q})
        if not r2: raise RuntimeError("No consultó estado BulkQuery")
        op = r2.json()["data"]["currentBulkOperation"]
        if op["status"] == "COMPLETED":
            return op["url"]
        if op["status"] in ("FAILED","CANCELED"):
            raise RuntimeError(f"BulkQuery terminó {op['status']}")
        # si sigue en curso, loop

def delete_products(gids):
    """Borra por REST todos los productos numéricos (extrae el ID final de cada GID)."""
    if not gids: 
        return
    print(f"🗑️ Borrando {len(gids)} productos…")
    def delete_one(gid):
        # gid = "gid://shopify/Product/123456" → 123456
        pid = gid.split("/")[-1]
        url = f"{REST_BASE}/products/{pid}.json"
        r = request_with_retry("DELETE", url)
        if not r or r.status_code not in (200,202,204,404):
            print(f"[ERROR] No borró {pid}: HTTP {r.status_code if r else 'NoResp'}")

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(delete_one, gids))
    print("🗑️ Borrado completo.")


def build_jsonl(productos):
    """Construye el JSONL listo para stagedUploadsCreate + bulkOperationRunMutation."""
    lines = []
    IVA = 21.0
    for p in productos:
        ean   = str(p.get("EAN","")).strip()
        sku   = str(p.get("REF","") or p.get("SKU","")).strip()
        title = p.get("NAME","")[:250]
        # cálculo de precio aproximado
        try:
            base   = float(p.get("PVD","0").replace(".","").replace(",",".")) \
                   + float(p.get("CANON","0").replace(".","").replace(",","."))
            margin = float(p.get("MARGIN","0").replace(".","").replace(",","."))
            price  = (base * (1+margin/100)) * (1+IVA/100)
            price  = f"{(int(price*100)/100):.2f}"
        except:
            price = "0.00"
        stock = int(float(p.get("STOCK","0")))
        desc  = p.get("DESCRIPTION","")
        img   = p.get("URL_IMG","")
        handle= f"ean-{ean or sku}"

        node = {
            "handle": handle,
            "title": title,
            "descriptionHtml": desc,
            "status": "ACTIVE",
            "productType": p.get("SUBFAMILIA",""),
            "tags": [IMPORT_TAG],
            "variants": [{
                "sku": sku,
                "barcode": ean,
                "price": price,
                "inventoryPolicy": "DENY",
                "inventoryItem": {"tracked": True},
                "inventoryQuantities": [{"locationId": LOCATION_ID, "availableQuantity": stock}]
            }]
        }
        if img:
            node["images"] = [{"src": img, "alt": title}]
        lines.append(json.dumps({"input": node}, ensure_ascii=False))
    print(f"🔨 Construidos {len(lines)} JSONL lines")
    return "\n".join(lines)


def staged_upload_and_bulk(jsonl_data):
    """1) stagedUploadsCreate → 2) subir JSONL → 3) bulkOperationRunMutation → 4) esperar"""
    # 1) stagedUploadsCreate
    create_q = '''
    mutation {
      stagedUploadsCreate(input:[
        {resource: BULK_MUTATION_VARIABLES, filename: "bulk.jsonl", mimeType:"text/jsonl", httpMethod:POST}
      ]) {
        stagedTargets { url parameters {name value} }
        userErrors { field message }
      }
    }
    '''
    r1 = request_with_retry("POST", GRAPHQL_URL, json={"query": create_q})
    data1 = r1.json()["data"]["stagedUploadsCreate"]
    if data1["userErrors"]:
        raise RuntimeError(f"stagedUploadsCreate errores: {data1['userErrors']}")
    tgt = data1["stagedTargets"][0]
    upload_url = tgt["url"]
    params     = {p["name"]:p["value"] for p in tgt["parameters"]}

    # 2) subir JSONL
    files = {"file": ("bulk.jsonl", jsonl_data.encode("utf-8"), "application/json")}
    r2 = session.post(upload_url, data=params, files=files)
    if r2.status_code not in (200,201,204):
        raise RuntimeError(f"Fallo al subir JSONL: HTTP {r2.status_code}")

    # 3) bulkOperationRunMutation
    mutation = '''
    mutation {
      bulkOperationRunMutation(
        mutation: "mutation($input: ProductInput!) { productCreate(input:$input){product{id}} }",
        stagedUploadPath: "%s"
      ) {
        bulkOperation { id status }
        userErrors { field message }
      }
    }''' % params["key"]
    r3 = request_with_retry("POST", GRAPHQL_URL, json={"query": mutation})
    data3 = r3.json()["data"]["bulkOperationRunMutation"]
    if data3["userErrors"]:
        raise RuntimeError(f"bulkOperationRunMutation errores: {data3['userErrors']}")
    bulk_id = data3["bulkOperation"]["id"]
    print(f"🚀 Bulk import iniciado (ID={bulk_id}), esperando finalizar…")

    # 4) esperar a COMPLETED y devolver URL
    while True:
        time.sleep(5)
        status_q = "query { currentBulkOperation{ status url errorCode }} "
        r4 = request_with_retry("POST", GRAPHQL_URL, json={"query": status_q})
        op = r4.json()["data"]["currentBulkOperation"]
        if op["status"] == "COMPLETED":
            return op["url"]
        if op["status"] in ("FAILED","CANCELED"):
            raise RuntimeError(f"Bulk import terminó {op['status']}")
        # sigue en curso…

def parse_created_ids(result_url):
    """Lee línea a línea el JSONL de respuesta y extrae los product.id creados."""
    r = request_with_retry("GET", result_url)
    ids = []
    if r and r.status_code==200:
        for L in r.text.splitlines():
            try:
                o = json.loads(L)
                pid = o.get("product",{}).get("id")
                if pid:
                    ids.append(pid)
            except:
                pass
    print(f"✅ Importados {len(ids)} productos")
    return ids


def publish_online(gids):
    """Publica cada GID al canal definido en PUBLICATION_ID."""
    if not gids: return
    print(f"📢 Publicando {len(gids)} en canal {PUBLICATION_ID}…")
    def pub_one(gid):
        q = '''
        mutation publish($id:ID!,$pubID:ID!){
          publishablePublish(id:$id,input:[{publicationId:$pubID}]){userErrors{message}}
        }
        '''
        vars = {"id": gid, "pubID": PUBLICATION_ID}
        r = request_with_retry("POST", GRAPHQL_URL, json={"query":q,"variables":vars})
        if not r or r.status_code>=400:
            print(f"[ERROR] No publicado {gid}: HTTP {r.status_code if r else 'NoResp'}")

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(pub_one, gids))
    print("📦 Publicación terminada.")


def main():
    try:
        externos = fetch_external()
        # 1) borrado
        url_del = run_bulk_query_to_delete()
        # descargar GIDs y eliminarlos
        gids = []
        if url_del:
            resp = request_with_retry("GET", url_del)
            for L in (resp.text or "").splitlines():
                try:
                    o=json.loads(L)
                    if "node" in o: gids.append(o["node"]["id"])
                    elif "id" in o: gids.append(o["id"])
                except:
                    pass
        delete_products(gids)

        # 2) preparar e importar
        jl = build_jsonl(externos)
        result_url = staged_upload_and_bulk(jl)
        created_ids = parse_created_ids(result_url)

        # 3) publicar
        publish_online(created_ids)

        print("🎉 Sincronización completada con éxito.")
        return 0

    except Exception as e:
        print("❌ ERROR FATAL:", e)
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
