#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de sincronización de productos Shopify mejorado para ejecutarse en GitHub Actions.
"""
import os
import json
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# Leer configuración desde variables de entorno (configuradas en GitHub Actions)
SHOPIFY_STORE = os.environ.get('SHOPIFY_STORE')  # Dominio de la tienda, e.g. 'mi-tienda.myshopify.com'
SHOPIFY_ACCESS_TOKEN = os.environ.get('SHOPIFY_ACCESS_TOKEN')  # Token de acceso Admin API (private o custom app)
API_VERSION = os.environ.get('SHOPIFY_API_VERSION', '2023-10')  # Versión de la API de Shopify a usar (por defecto 2023-10)

# URL base para API Admin de Shopify
if not SHOPIFY_STORE:
    raise EnvironmentError("Debe especificar SHOPIFY_STORE (dominio .myshopify.com) en las variables de entorno")
SHOPIFY_GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/graphql.json"
SHOPIFY_REST_BASE_URL = f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}"

# Configuración del catálogo externo
EXTERNAL_CATALOG_URL = os.environ.get('EXTERNAL_CATALOG_URL')  # URL para obtener productos; puede contener "{subfamily}" si se usa por subfamilia
EXTERNAL_CATALOG_SUBFAMILIAS = os.environ.get('EXTERNAL_CATALOG_SUBFAMILIAS')  # Lista de subfamilias separados por coma (opcional)
EXTERNAL_CATALOG_TIMEOUT = int(os.environ.get('EXTERNAL_CATALOG_TIMEOUT', '30'))  # Timeout para requests al catálogo externo

# Valores por defecto para campos del producto (en caso de faltar en los datos externos)
DEFAULT_VENDOR = os.environ.get('DEFAULT_VENDOR', 'Importado')  # Vendor (proveedor) por defecto si no se especifica
DEFAULT_PRODUCT_TYPE = os.environ.get('DEFAULT_PRODUCT_TYPE', '')  # Tipo de producto por defecto (opcional)

# Etiqueta para marcar productos importados (debe coincidir con la usada para eliminarlos)
IMPORT_TAG = os.environ.get('IMPORT_TAG', 'ImportadoAPI')

# Configuración de reintentos para solicitudes HTTP
MAX_RETRIES = 3  # número máximo de reintentos
RETRY_BACKOFF = 5  # segundos base de espera para reintentos (puede aumentar exponencialmente)

# Iniciar sesión de requests global
session = requests.Session()
session.headers.update({
    'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
    'Content-Type': 'application/json'
})

# Listas para recopilar IDs que fallaron en eliminación o publicación (para reporte final)
failed_deletions = []
failed_publications = []

# Mejora: Implementamos reintentos para peticiones HTTP a Shopify y catálogo externo
def request_with_retry(method, url, **kwargs):
    """Realiza una solicitud HTTP con reintentos en caso de errores transitorios."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.request(method, url, timeout=EXTERNAL_CATALOG_TIMEOUT, **kwargs)
        except requests.exceptions.RequestException as e:
            # Error de conexión (RemoteDisconnected, timeout, etc.)
            if attempt < MAX_RETRIES:
                print(f"[WARN] Error de conexión en intento {attempt} para {url}: {e}. Reintentando...")
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            else:
                print(f"[ERROR] No se pudo conectar a {url} tras {attempt} intentos: {e}")
                return None
        # Si obtenemos una respuesta, manejar códigos de error HTTP
        status = response.status_code
        if status >= 500 or status == 429:
            # Errores del servidor o límite de peticiones (429 Too Many Requests)
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                print(f"[WARN] Respuesta HTTP {status} para {url} en intento {attempt}. Esperando {wait}s para reintentar...")
                time.sleep(wait)
                continue
        # Código 404 en eliminación lo consideramos éxito (producto ya eliminado) y no reintentamos
        if status == 404 and method.upper() == 'DELETE':
            return response
        # Código 200-299 OK, o 404 (para GET/POST no aplica) => devolver respuesta
        if status < 300:
            return response
        else:
            # Otros códigos de error (400-499 excepto 429, o 500+ después de agotar reintentos)
            if attempt < MAX_RETRIES:
                print(f"[ERROR] Respuesta HTTP {status} en intento {attempt} para {url}. Último intento.")
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            print(f"[ERROR] Respuesta HTTP {status} para {url}. No se reintentará más.")
            return response
    return None

def fetch_external_products():
    """Descarga el catálogo externo (posiblemente dividido por subfamilias) y retorna la lista de productos."""
    products = []
    if not EXTERNAL_CATALOG_URL:
        raise EnvironmentError("No se especificó la URL del catálogo externo (EXTERNAL_CATALOG_URL)")
    subfamilias = []
    if EXTERNAL_CATALOG_SUBFAMILIAS:
        # Obtener lista de subfamilias desde variable de entorno (separadas por coma)
        subfamilias = [s.strip() for s in EXTERNAL_CATALOG_SUBFAMILIAS.split(',') if s.strip()]
    if subfamilias:
        print(f"Descargando catálogo externo para {len(subfamilias)} subfamilias...")
        # Descargar cada subfamilia en paralelo para eficiencia
        def fetch_subfamily(sub):
            url = EXTERNAL_CATALOG_URL.replace('{subfamily}', sub)
            resp = request_with_retry('GET', url)
            if resp is None:
                print(f"[ERROR] Falló descarga de subfamilia {sub}")
                return []
            if resp.status_code != 200:
                print(f"[ERROR] HTTP {resp.status_code} obteniendo subfamilia {sub}")
                return []
            try:
                data = resp.json()
            except Exception as e:
                print(f"[ERROR] No se pudo parsear JSON para subfamilia {sub}: {e}")
                return []
            # Asumimos que la respuesta contiene directamente lista de productos o dict con 'products'
            if isinstance(data, dict) and 'products' in data:
                return data['products']
            elif isinstance(data, list):
                return data
            else:
                return data  # Estructura inesperada, devolver tal cual
        with ThreadPoolExecutor(max_workers=min(5, len(subfamilias))) as executor:
            futures = {executor.submit(fetch_subfamily, sub): sub for sub in subfamilias}
            for future in as_completed(futures):
                sub = futures[future]
                try:
                    result = future.result()
                    if result:
                        products.extend(result)
                except Exception as exc:
                    print(f"[ERROR] Excepción al obtener subfamilia {sub}: {exc}")
    else:
        # Si no se especificaron subfamilias, asumimos que la URL devuelve todo el catálogo
        print("Descargando catálogo externo completo...")
        resp = request_with_retry('GET', EXTERNAL_CATALOG_URL)
        if resp is None or resp.status_code != 200:
            raise RuntimeError(f"Falló la descarga del catálogo externo: {resp.status_code if resp else 'No Response'}")
        try:
            data = resp.json()
        except Exception as e:
            raise RuntimeError(f"El catálogo externo no devolvió JSON válido: {e}")
        if isinstance(data, dict) and 'products' in data:
            products = data['products']
        elif isinstance(data, list):
            products = data
        else:
            products = data
    # Mejora: Eliminación de duplicados por SKU/EAN en datos externos
    seen_keys = set()
    unique_products = []
    for prod in products:
        # Determinar clave única (SKU o EAN)
        key = None
        for field in prod.keys():
            fname = field.lower()
            if fname in ('sku', 'ean', 'ean13', 'barcode'):
                key = str(prod[field]).strip()
                break
        if not key:
            # Si no hay SKU/EAN, usar título u otra representación
            key = prod.get('title') or prod.get('nombre') or json.dumps(prod)
        if key in seen_keys:
            continue  # Producto duplicado encontrado, lo omitimos
        seen_keys.add(key)
        unique_products.append(prod)
    print(f"Productos externos obtenidos: {len(unique_products)} (tras eliminar duplicados)")
    return unique_products

def delete_existing_products(product_ids):
    """Elimina productos existentes en Shopify dado una lista de IDs (GraphQL GIDs o números)."""
    if not product_ids:
        return
    print(f"Eliminando {len(product_ids)} productos existentes (etiquetados '{IMPORT_TAG}')...")
    # Convertir GIDs GraphQL a IDs numéricos
    ids_numeric = []
    for gid in product_ids:
        if isinstance(gid, str) and gid.startswith('gid://'):
            try:
                ids_numeric.append(int(gid.split('/')[-1]))
            except:
                ids_numeric.append(gid)
        else:
            ids_numeric.append(gid)
    def delete_one(pid):
        url = f"{SHOPIFY_REST_BASE_URL}/products/{pid}.json"
        resp = request_with_retry('DELETE', url)
        if resp is None:
            failed_deletions.append(pid)
            print(f"[ERROR] Fallo al eliminar producto ID {pid}")
        elif resp.status_code in (200, 202, 204, 404):
            return  # eliminado o no existía
        else:
            failed_deletions.append(pid)
            print(f"[ERROR] No se eliminó el producto ID {pid}. Código HTTP: {resp.status_code}")
    # Mejora: Eliminación concurrente para acelerar la ejecución
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(delete_one, ids_numeric))
    if failed_deletions:
        print(f"Advertencia: {len(failed_deletions)} productos no se pudieron eliminar tras reintentos.")

def run_bulk_query_for_deletion():
    """Ejecuta un Bulk Query GraphQL para obtener IDs de todos los productos con la etiqueta de importación."""
    query = f"""
    mutation {{
      bulkOperationRunQuery(query: \"\"\"
        {{
          products(query: "tag:{IMPORT_TAG}") {{ edges {{ node {{ id }} }} }}
        }}
      \"\"\") {{
        bulkOperation {{ id status }}
        userErrors {{ field message }}
      }}
    }}
    """
    resp = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json={'query': query})
    if resp is None:
        raise RuntimeError("Falló la solicitud bulkOperationRunQuery inicial")
    data = resp.json()
    errors = data.get('errors') or data.get('data', {}).get('bulkOperationRunQuery', {}).get('userErrors')
    if errors:
        raise RuntimeError(f"Error al iniciar bulkOperationRunQuery: {errors}")
    bulk_op = data['data']['bulkOperationRunQuery']['bulkOperation']
    bulk_id = bulk_op['id']
    print(f"BulkOperationRunQuery iniciado (ID={bulk_id}), esperando resultados...")
    result_url = None
    while True:
        time.sleep(2)
        status_query = "query { currentBulkOperation { status, url, errorCode, objectCount } }"
        resp_status = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json={'query': status_query})
        if resp_status is None:
            raise RuntimeError("Falló al consultar estado de BulkOperation")
        status_data = resp_status.json()
        current_op = status_data.get('data', {}).get('currentBulkOperation')
        if not current_op:
            raise RuntimeError("No se pudo obtener información de BulkOperation actual")
        status = current_op['status']
        if status == 'COMPLETED':
            result_url = current_op.get('url')
            break
        elif status in ('FAILED', 'CANCELED'):
            err_code = current_op.get('errorCode')
            raise RuntimeError(f"BulkOperationRunQuery falló con estado {status}. Código de error: {err_code}")
        # Si continúa en curso, seguir esperando
    product_ids = []
    if result_url:
        resp_file = request_with_retry('GET', result_url)
        if resp_file and resp_file.status_code == 200:
            for line in resp_file.text.splitlines():
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                pid = None
                if isinstance(obj, dict):
                    if 'id' in obj and isinstance(obj['id'], str):
                        pid = obj['id']
                    elif 'node' in obj and 'id' in obj['node']:
                        pid = obj['node']['id']
                if pid:
                    product_ids.append(pid)
        else:
            print(f"[ERROR] No se pudo obtener el resultado de BulkOperation (status code {resp_file.status_code if resp_file else 'NoResponse'})")
    print(f"Encontrados {len(product_ids)} productos para eliminar.")
    return product_ids

def prepare_bulk_import_file(products):
    """Prepara el contenido JSONL para la importación bulk de nuevos productos."""
    lines = []
    for prod in products:
        product_input = {}
        # Campos básicos obligatorios
        title = prod.get('title') or prod.get('nombre') or prod.get('Name') or prod.get('nombreProducto')
        if not title:
            print(f"[WARN] Producto omitido por falta de título: {prod}")
            continue
        product_input['title'] = str(title)[:255]
        product_input['status'] = 'ACTIVE'  # establecer producto como activo (para poder publicarlo)
        # Vendor y ProductType
        vendor = prod.get('vendor') or prod.get('marca') or prod.get('brand') or DEFAULT_VENDOR
        product_input['vendor'] = str(vendor)[:255]
        if DEFAULT_PRODUCT_TYPE or prod.get('productType') or prod.get('tipo'):
            product_input['productType'] = prod.get('productType') or prod.get('tipo') or DEFAULT_PRODUCT_TYPE
        # Body HTML (descripción)
        body = prod.get('body_html') or prod.get('bodyHtml') or prod.get('descripcion') or prod.get('description')
        if body:
            product_input['bodyHtml'] = str(body)
        # Tags: incluir la etiqueta de importación y cualquier otra del producto
        tags = []
        if prod.get('tags'):
            if isinstance(prod['tags'], str):
                tags = [t.strip() for t in prod['tags'].split(',')]
            elif isinstance(prod['tags'], list):
                tags = prod['tags']
        if IMPORT_TAG not in tags:
            tags.append(IMPORT_TAG)
        product_input['tags'] = tags
        # Variantes
        variants = []
        if prod.get('variants'):
            for var in prod['variants']:
                variant_input = {}
                if 'price' in var:
                    variant_input['price'] = str(var['price'])
                elif 'precio' in var:
                    variant_input['price'] = str(var['precio'])
                if 'sku' in var or 'SKU' in var:
                    variant_input['sku'] = var.get('sku') or var.get('SKU')
                if 'barcode' in var or 'ean' in var or 'EAN' in var or 'ean13' in var:
                    variant_input['barcode'] = var.get('barcode') or var.get('ean') or var.get('EAN') or var.get('ean13')
                # Opciones de variante (e.g. tallas, colores)
                if 'option1' in var or 'option2' in var or 'option3' in var:
                    for opt in ['option1', 'option2', 'option3']:
                        if opt in var:
                            variant_input[opt] = str(var[opt])
                variants.append(variant_input)
        else:
            variant_input = {}
            price = prod.get('price') or prod.get('precio')
            variant_input['price'] = str(price) if price is not None else '0.0'
            sku = prod.get('sku') or prod.get('SKU')
            if sku:
                variant_input['sku'] = str(sku)
            barcode = prod.get('barcode') or prod.get('ean') or prod.get('EAN') or prod.get('ean13')
            if barcode:
                variant_input['barcode'] = str(barcode)
            variants.append(variant_input)
        product_input['variants'] = variants
        # Imágenes
        images = []
        if prod.get('images'):
            for img in prod['images']:
                if isinstance(img, str):
                    images.append({'src': img})
                elif isinstance(img, dict) and 'src' in img:
                    images.append({'src': img['src']})
        elif prod.get('image'):
            img = prod['image']
            if isinstance(img, str):
                images.append({'src': img})
            elif isinstance(img, dict) and 'src' in img:
                images.append({'src': img['src']})
        if images:
            product_input['images'] = images
        lines.append(json.dumps({"input": product_input}, ensure_ascii=False))
    return "\n".join(lines)

def execute_bulk_product_import(jsonl_data):
    """Ejecuta la carga bulk de productos usando GraphQL (stagedUploadsCreate y bulkOperationRunMutation)."""
    # Paso 1: obtener URL de subida con stagedUploadsCreate
    staged_query = """
    mutation GenerateUploadURL {
      stagedUploadsCreate(input: {
        resource: BULK_MUTATION_VARIABLES,
        filename: \\"bulk_products.jsonl\\",
        mimeType: \\"text/jsonl\\",
        httpMethod: POST
      }) {
        userErrors { field message }
        stagedTargets { url resourceUrl parameters { name value } }
      }
    }
    """
    resp = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json={'query': staged_query})
    if resp is None:
        raise RuntimeError("Error al solicitar stagedUploadsCreate")
    data = resp.json()
    errors = data.get('errors') or data.get('data', {}).get('stagedUploadsCreate', {}).get('userErrors')
    if errors:
        raise RuntimeError(f"Errores en stagedUploadsCreate: {errors}")
    targets = data['data']['stagedUploadsCreate']['stagedTargets']
    if not targets:
        raise RuntimeError("No se obtuvo URL de carga (stagedTargets vacío)")
    target = targets[0]
    upload_url = target['url']
    params = { item['name']: item['value'] for item in target['parameters'] }
    files = {'file': ('bulk_products.jsonl', jsonl_data, 'text/jsonl')}
    resp_upload = requests.post(upload_url, data=params, files=files)
    if resp_upload.status_code != 204:
        raise RuntimeError(f"Falló la subida del archivo JSONL (código HTTP {resp_upload.status_code})")
    # Paso 2: Ejecutar bulkOperationRunMutation para importar los productos
    mutation_string = "mutation call($input: ProductInput!) { productCreate(input: $input) { product { id } userErrors { message field } } }"
    bulk_mutation_query = {
        'query': f"""
        mutation {{
          bulkOperationRunMutation(
            mutation: "{mutation_string}",
            stagedUploadPath: "{params.get('key')}"
          ) {{
            bulkOperation {{ id status }}
            userErrors {{ field message }}
          }}
        }}
        """
    }
    resp2 = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json=bulk_mutation_query)
    if resp2 is None:
        raise RuntimeError("Error al iniciar bulkOperationRunMutation")
    data2 = resp2.json()
    errors2 = data2.get('errors') or data2.get('data', {}).get('bulkOperationRunMutation', {}).get('userErrors')
    if errors2 and len(errors2) > 0:
        raise RuntimeError(f"Error al iniciar bulkOperationRunMutation: {errors2}")
    bulk_op = data2['data']['bulkOperationRunMutation']['bulkOperation']
    bulk_id = bulk_op['id']
    print(f"BulkOperationRunMutation iniciado (ID={bulk_id}), esperando a que finalice...")
    result_url = None
    while True:
        time.sleep(5)
        status_query = "query { currentBulkOperation { status, url, errorCode, objectCount } }"
        resp_status = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json={'query': status_query})
        if resp_status is None:
            raise RuntimeError("Falló al consultar estado de BulkOperation (importación)")
        status_data = resp_status.json()
        current_op = status_data.get('data', {}).get('currentBulkOperation')
        if not current_op:
            continue
        status = current_op['status']
        if status == 'COMPLETED':
            result_url = current_op.get('url')
            break
        elif status in ('FAILED', 'CANCELED'):
            err_code = current_op.get('errorCode')
            raise RuntimeError(f"La importación bulk falló con estado {status}. Código: {err_code}")
        # Si está en curso, seguir esperando
    created_product_ids = []
    if result_url:
        resp_file = request_with_retry('GET', result_url)
        if resp_file and resp_file.status_code == 200:
            for line in resp_file.text.splitlines():
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if 'product' in obj and obj['product'] and 'id' in obj['product']:
                    created_product_ids.append(obj['product']['id'])
                else:
                    if obj.get('userErrors'):
                        err = obj['userErrors']
                        print(f"[ERROR] Error en creación de un producto: {err}")
        else:
            print(f"[ERROR] No se pudo descargar el resultado de BulkOperation de importación (status {resp_file.status_code if resp_file else 'NoResponse'})")
    print(f"Productos importados exitosamente: {len(created_product_ids)}")
    return created_product_ids

def publish_products(product_ids):
    """Publica los productos en la tienda online (Online Store) dado sus IDs."""
    if not product_ids:
        return
    print(f"Publicando {len(product_ids)} productos en la tienda online...")
    pub_query = "query { publications(first:5) { edges { node { id publicationSalesChannel { name } } } } }"
    resp = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json={'query': pub_query})
    if resp is None:
        raise RuntimeError("No se pudo obtener la lista de publications")
    data = resp.json()
    pub_edges = data.get('data', {}).get('publications', {}).get('edges', [])
    if not pub_edges:
        raise RuntimeError("No se encontró ninguna publicación de canal de ventas")
    publication_id = pub_edges[0]['node']['id']  # asumir que la primera es la tienda online
    def publish_one(gid):
        query = "mutation PublishProduct($pubId: ID!, $prodId: ID!) { publishablePublish(input: {publicationId: $pubId, id: $prodId}) { userErrors { message } } }"
        variables = {"pubId": publication_id, "prodId": gid}
        resp_local = request_with_retry('POST', SHOPIFY_GRAPHQL_URL, json={'query': query, 'variables': variables})
        if resp_local is None:
            failed_publications.append(gid)
            print(f"[ERROR] Fallo al publicar producto {gid}")
        else:
            data_local = resp_local.json()
            errors_local = data_local.get('errors') or data_local.get('data', {}).get('publishablePublish', {}).get('userErrors')
            if errors_local:
                print(f"[WARN] Error al publicar producto {gid}: {errors_local}")
                return
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(publish_one, product_ids))
    if failed_publications:
        print(f"Advertencia: {len(failed_publications)} productos no se pudieron publicar tras reintentos.")

def main():
    # 1. Descargar catálogo externo
    try:
        products = fetch_external_products()
    except Exception as e:
        print(f"[ERROR] No se pudo obtener el catálogo externo: {e}")
        return 1
    if not products:
        print("[ERROR] No se encontraron productos en el catálogo externo. Abortando.")
        return 1
    # 2. Consultar y eliminar productos existentes con la etiqueta de importación
    try:
        ids_to_delete = run_bulk_query_for_deletion()
    except Exception as e:
        print(f"[ERROR] Falló la obtención de productos a eliminar: {e}")
        return 1
    delete_existing_products(ids_to_delete)
    # 3. Preparar y ejecutar la importación en bloque de nuevos productos
    jsonl_data = prepare_bulk_import_file(products)
    if not jsonl_data:
        print("[ERROR] No se pudo generar datos para importación (quizás no hay productos válidos). Abortando.")
        return 1
    created_ids = []
    try:
        created_ids = execute_bulk_product_import(jsonl_data)
    except Exception as e:
        print(f"[ERROR] Falló la importación bulk de productos: {e}")
        traceback.print_exc()
        return 1
    # 4. Publicar automáticamente los productos importados en la tienda online
    try:
        publish_products(created_ids)
    except Exception as e:
        print(f"[ERROR] Falló la publicación de productos: {e}")
    print("Sincronización completada.")
    return 0

if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)
