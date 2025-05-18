import os
import requests
import json

# Configuración de la API de Shopify
SHOP_NAME = os.getenv("SHOP_NAME")  # e.g. "mi-tienda"
API_VERSION = "2023-10"
ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")  # token de acceso privado a la API de Admin
GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"

# Fase 1: Eliminar productos importados previamente con la etiqueta "ImportadoAPI"
def eliminar_productos_importados():
    """
    Elimina de Shopify todos los productos que tengan la etiqueta 'ImportadoAPI'.
    """
    query = '''
    query($cursor: String) {
      products(first: 250, query: "tag:ImportadoAPI", after: $cursor) {
        edges {
          node { id }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    '''
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": ACCESS_TOKEN}
    productos_a_eliminar = []

    # Paginación para obtener todos los IDs de productos con la etiqueta ImportadoAPI
    cursor = None
    while True:
        variables = {"cursor": cursor}
        resp = requests.post(GRAPHQL_URL, json={"query": query, "variables": variables}, headers=headers)
        data = resp.json()
        # Verificar errores en la respuesta
        if "errors" in data:
            raise Exception(f"Error al consultar productos a eliminar: {data['errors']}")
        productos = data["data"]["products"]
        for edge in productos["edges"]:
            productos_a_eliminar.append(edge["node"]["id"])
        if productos["pageInfo"]["hasNextPage"]:
            cursor = productos["pageInfo"]["endCursor"]
        else:
            break

    # Eliminar cada producto obtenido
    if not productos_a_eliminar:
        print("No hay productos ImportadoAPI previos para eliminar.")
    else:
        for product_id in productos_a_eliminar:
            mutation = '''
            mutation($input: ProductDeleteInput!) {
              productDelete(input: $input) {
                deletedProductId
                userErrors { field message }
              }
            }
            '''
            variables = {"input": {"id": product_id}}
            resp = requests.post(GRAPHQL_URL, json={"query": mutation, "variables": variables}, headers=headers)
            result = resp.json()
            if "errors" in result or result.get("data", {}).get("productDelete", {}).get("userErrors"):
                # Si ocurre un error al eliminar, mostramos mensaje pero continuamos con los demás
                mensaje_error = result.get("errors") or result["data"]["productDelete"]["userErrors"]
                print(f"Error eliminando producto {product_id}: {mensaje_error}")
        print(f"Eliminados {len(productos_a_eliminar)} productos con etiqueta ImportadoAPI.")

# Fase 2: Obtener productos de la API externa y construir archivo JSONL para Bulk API
def obtener_productos_externos():
    """
    Llama a la API externa por subfamilias y recopila todos los productos en una lista.
    Retorna una lista de productos (dicts) obtenidos de la fuente externa.
    """
    productos_fuente = []
    subfamilias = ["subfamilia1", "subfamilia2", "subfamilia3"]  # Ejemplo de subfamilias a consultar
    for subfamilia in subfamilias:
        url = f"https://api.externo.com/productos/{subfamilia}"  # URL de la API externa para la subfamilia
        resp = requests.get(url)
        if resp.status_code == 200:
            data = resp.json()
            # Suponiendo que la respuesta contiene una lista de productos bajo la clave 'products'
            productos_fuente.extend(data.get("products", []))
        else:
            print(f"Advertencia: Falló la consulta de la subfamilia {subfamilia} (status {resp.status_code})")
    return productos_fuente

def construir_jsonl_productos(productos):
    """
    Construye el contenido JSONL (línea por línea en formato JSON) para la importación Bulk de productos.
    Retorna el nombre de archivo del JSONL generado.
    """
    jsonl_filename = "productos_import.jsonl"
    with open(jsonl_filename, "w", encoding="utf-8") as jsonl_file:
        for producto in productos:
            # Construir la mutación GraphQL para crear/actualizar un producto
            titulo = producto.get("titulo")  # ajuste según las claves reales del JSON externo
            descripcion = producto.get("descripcion_html")  # HTML de descripción
            precio = producto.get("precio")  # precio del producto
            sku = producto.get("sku")
            stock = producto.get("stock", 0)
            imagen_url = producto.get("imagen_url")  # URL de una imagen principal
            tags = producto.get("tags", [])
            # Asegurar que la etiqueta ImportadoAPI esté presente
            if "ImportadoAPI" not in tags:
                tags.append("ImportadoAPI")

            # Construir el payload JSON para la mutación productCreate (o productUpsert/productUpdate según corresponda)
            product_input = {
                "title": titulo,
                "bodyHtml": descripcion,
                "status": "ACTIVE",           # Publicar directamente el producto (estado activo)
                "tags": tags,
                "variants": [
                    {
                        "sku": sku,
                        "price": str(precio) if precio is not None else "0.0",
                        "inventoryQuantity": stock
                    }
                ]
            }
            if imagen_url:
                # Agregar imagen si existe URL
                product_input["images"] = [{"src": imagen_url}]

            # Crear la línea JSON (codificar product_input como JSON dentro de la mutación GraphQL)
            mutation_line = {
                "id": None,
                "operation": "mutation productCreate($input: ProductInput!) { productCreate(input: $input) { product { id } userErrors { message } } }",
                "input": product_input
            }
            # Escribir la línea en formato JSONL
            jsonl_file.write(json.dumps(mutation_line, ensure_ascii=False) + "\n")
    return jsonl_filename

# Fase 3: Subir el archivo JSONL a Shopify (staged upload) e iniciar la operación Bulk
def ejecutar_bulk_import(jsonl_filename):
    # Paso 3.1: Solicitar URLs de subida mediante stagedUploadsCreate
    mutation = '''
    mutation {
      stagedUploadsCreate(input: {
        resource: BULK_MUTATION_VARIABLES,
        filename: "%s",
        mimeType: "text/plain"
      }) {
        userErrors { field message }
        stagedTargets {
          url
          resourceUrl
          parameters { name value }
        }
      }
    }
    ''' % jsonl_filename
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": ACCESS_TOKEN}
    resp = requests.post(GRAPHQL_URL, json={"query": mutation}, headers=headers)
    data = resp.json()
    if "errors" in data or data.get("data", {}).get("stagedUploadsCreate", {}).get("userErrors"):
        raise Exception(f"Error al solicitar staged upload: {data}")
    staged_target = data["data"]["stagedUploadsCreate"]["stagedTargets"][0]
    upload_url = staged_target["url"]
    resource_url = staged_target["resourceUrl"]
    # Parámetros para la subida (campos de formulario)
    form_params = { param["name"]: param["value"] for param in staged_target["parameters"] }

    # Paso 3.2: Subir el archivo JSONL al URL proporcionado (Amazon S3)
    with open(jsonl_filename, "rb") as f:
        files = {'file': (jsonl_filename, f, 'text/plain')}
        # Realizar POST al upload_url con los form params y el archivo
        upload_resp = requests.post(upload_url, data=form_params, files=files)
        if upload_resp.status_code != 204:
            raise Exception(f"Error subiendo el archivo JSONL a S3: {upload_resp.status_code} - {upload_resp.text}")

    # Paso 3.3: Iniciar la operación Bulk con bulkOperationRunMutation utilizando el resourceUrl del archivo subido
    bulk_mutation = '''
    mutation {
      bulkOperationRunMutation(
        mutation: "mutation importProducts($input: ProductInput!) { productCreate(input: $input) { product { id } userErrors { message } } }",
        stagedUploadPath: "%s"
      ) {
        bulkOperation {
          id
          status
        }
        userErrors { field message }
      }
    }
    ''' % resource_url
    resp = requests.post(GRAPHQL_URL, json={"query": bulk_mutation}, headers=headers)
    result = resp.json()
    if "errors" in result or result.get("data", {}).get("bulkOperationRunMutation", {}).get("userErrors"):
        raise Exception(f"Error al iniciar Bulk Operation: {result}")

    bulk_op = result["data"]["bulkOperationRunMutation"]["bulkOperation"]
    print(f"Bulk operation iniciada (ID: {bulk_op['id']}, status: {bulk_op['status']}).")

    # (Opcional) Paso 3.4: Consultar el estado hasta que finalice
    # Aquí podríamos agregar lógica para hacer polling del estado de la operación bulk hasta que sea "COMPLETED"
    # por simplicidad, asumimos que el proceso se completa externamente (por ejemplo, esperando a que Shopify lo procese).
    return bulk_op["id"]

# Fase 4: (Opcional) Publicar los nuevos productos en el canal online si no se hizo en la creación
# Nota: En este caso hemos establecido status = "ACTIVE" en la creación, lo que publica el producto.
# Si necesitáramos asegurarnos de la publicación en un canal específico, usaríamos publishablePublish con el ID del canal online.

if __name__ == "__main__":
    # 1. Eliminar productos antiguos importados
    eliminar_productos_importados()
    # 2. Obtener datos de productos desde la API externa
    productos_nuevos = obtener_productos_externos()
    # 3. Construir archivo JSONL con las mutaciones de creación de productos
    jsonl_file = construir_jsonl_productos(productos_nuevos)
    # 4. Ejecutar la importación masiva (Bulk API) de los nuevos productos
    try:
        bulk_id = ejecutar_bulk_import(jsonl_file)
        print(f"Importación Bulk iniciada. ID de operación: {bulk_id}")
    except Exception as e:
        print(f"Error en la importación Bulk: {e}")
