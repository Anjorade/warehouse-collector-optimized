import os
import requests
import json
import time
from datetime import datetime
from urllib.parse import quote

# ======================================
# CONFIGURACIÓN (VARIABLES DE ENTORNO)
# ======================================
TOKEN = os.getenv("API_TOKEN")  # Desde GitHub Secrets
BASE_URL = os.getenv("API_BASE_URL")  # Desde GitHub Secrets
HEADERS = {"token": TOKEN}
WAREHOUSES = json.loads(os.getenv("WAREHOUSES", '[]'))  # Lista opcional de almacenes

# Configuración de ejecución
MAX_RETRIES = 2           # Reintentos por consulta fallida
REQUEST_DELAY = 30        # Espera entre consultas (segundos)
RETRY_DELAY = 10          # Espera entre reintentos (segundos)

# ======================================
# DEFINICIÓN DE CONSULTAS (MODIFICABLE)
# ======================================
QUERIES = [
    {
        "id": "Consulta_1",
        "output_file": "Consulta_1.json",
        "endpoint": "/System.MaterialTransactions.List.View1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1145' and not (ctxn_primary_uom_code ilike 'Und'"
        },
        "use_warehouse": False  # Cambiar a True si se requiere filtro por almacén
    },
    {
        "id": "Consulta_2",
        "output_file": "Consulta_2.json",
        "endpoint": "/System.MaterialTransactions.List.View1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1145' and ctxn_primary_uom_code ilike 'Und'"
        },
        "use_warehouse": False  # Este sí usa filtro por almacén
    },
    {
        "id": "Consulta_3",
        "output_file": "Consulta_2.json",
        "endpoint": "/System.MaterialTransactions.List.View1",
        "params": {
            "orderby": "ctxn_transaction_date desc",
            "take": "30000",
            "where": "ctxn_movement_type ilike '261%25%' and (ctxn_transaction_date > current_date - 120) and ctxn_warehouse_code ilike '1290'"
        },
        "use_warehouse": False  # Este sí usa filtro por almacén
    }
]

# ======================================
# FUNCIONES PRINCIPALES (ACTUALIZADAS)
# ======================================
def build_query_url(query_config, warehouse=None):
    """Construye URL con codificación segura y warehouse opcional"""
    params = query_config["params"].copy()
    
    # Añade filtro por warehouse si está configurado y se proporciona
    if query_config.get("use_warehouse", False) and warehouse:
        if "where" in params:
            params["where"] = f"{params['where']} and ctxn_warehouse_code ilike '{warehouse}'"
        else:
            params["where"] = f"ctxn_warehouse_code ilike '{warehouse}'"
    
    # Manejo especial de caracteres en el parámetro where
    if 'where' in params:
        params['where'] = params['where'].replace(' ', '%20').replace("'", "%27")
    
    # Construye la URL manualmente para mejor control
    url = f"{BASE_URL}{query_config['endpoint']}?orderby={params['orderby']}&take={params['take']}"
    if 'where' in params:
        url += f"&where={params['where']}"
    
    return url

def execute_query(query_config, warehouse=None):
    """Ejecuta consulta con manejo robusto de errores"""
    url = build_query_url(query_config, warehouse)
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"\n▶️ Ejecutando {query_config['id']} (Intento {attempt + 1})")
            if warehouse:
                print(f"   Almacén: {warehouse}")
            
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()  # Lanza error para códigos 4XX/5XX
            
            data = response.json()
            if not data:
                print(f"⚠️  {query_config['id']} devolvió datos vacíos")
                return None
                
            # Añadir metadatos básicos
            for item in data:
                item['_timestamp_carga'] = datetime.now().isoformat()
                item['_query_id'] = query_config['id']
                if warehouse:
                    item['_warehouse'] = warehouse
            
            print(f"✅ {query_config['id']} - {len(data)} registros obtenidos")
            return data
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                print(f"❌ {query_config['id']} falló después de {MAX_RETRIES} reintentos: {str(e)}")
                return None
            print(f"⏳ Esperando {RETRY_DELAY}s antes de reintentar...")
            time.sleep(RETRY_DELAY)

def save_data(data, filename):
    """Guarda datos en archivo JSON"""
    if not data:
        return False
        
    try:
        os.makedirs("data", exist_ok=True)
        with open(f"data/{filename}", 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Datos guardados en data/{filename}")
        return True
    except Exception as e:
        print(f"❌ Error guardando {filename}: {str(e)}")
        return False

def process_queries():
    """Orquesta la ejecución de todas las consultas"""
    print("\n🔷 INICIANDO CONSULTAS PARA POWER BI 🔷")
    start_time = time.time()
    has_errors = False
    
    try:
        for query in QUERIES:
            # Consultas sin warehouse
            if not query.get("use_warehouse", False):
                data = execute_query(query)
                if data:
                    save_data(data, query["output_file"])
                else:
                    has_errors = True
            # Consultas con warehouse
            else:
                if not WAREHOUSES:
                    print(f"⚠️ Consulta {query['id']} requiere warehouse pero no hay almacenes configurados")
                    continue
                    
                for warehouse in WAREHOUSES:
                    data = execute_query(query, warehouse)
                    if data:
                        save_data(data, query["output_file"])
                    else:
                        has_errors = True
            
            # Pausa entre consultas (excepto la última)
            if query != QUERIES[-1]:
                time.sleep(REQUEST_DELAY)
    
    finally:
        duration = (time.time() - start_time) / 60
        print(f"\n⏱️ Tiempo total: {duration:.2f} minutos")
        if has_errors:
            print("🔴 Finalizado con errores")
            exit(1)
        print("🟢 Ejecución completada con éxito")

if __name__ == "__main__":
    process_queries()
