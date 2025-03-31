import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote
from dotenv import load_dotenv  # Para desarrollo local

# Carga variables de entorno
load_dotenv()

# ======================
# CONFIGURACIÓN PRINCIPAL
# ======================
TOKEN = os.getenv("API_TOKEN")
BASE_URL = os.getenv("API_BASE_URL")  # Desde secrets
HEADERS = {"token": TOKEN}
WAREHOUSE_CODES = ['1145', '1290']  # Almacenes a procesar

# Configuración de reintentos
MAX_RETRIES = 2           # Número máximo de reintentos
REQUEST_DELAY = 30        # Segundos entre consultas diferentes
RETRY_DELAY = 10          # Segundos entre reintentos

# ======================
# DEFINICIÓN DE ENDPOINTS
# ======================
ENDPOINTS = {
    "sales_orders": "/Ardisa.SalesOrders.List.View1",
    "goods_receipts": "/System.GoodsRecipts.List.View1",
    "goods_issues": "/System.GoodsIssues.List.View1",
    "inbound_deliveries": "/Ardisa.InboundDeliveries.List.View1",
    "outbound_deliveries": "/System.OutboundDeliveries.List.View1"  # Nuevo
}

# ======================
# CONFIGURACIÓN DE QUERIES
# ======================
QUERY_CONFIG = [
    {
        "name": "sales_orders",
        "params": {
            "orderby": "cslo_created_on desc",
            "take": 20000,
            "where": "cwhs_code ilike '{warehouse}' and (cslo_created_on > current_date -182)"
        }
    },
    {
        "name": "goods_receipts",
        "params": {
            "orderby": "cgre_created_on desc",
            "take": 4000,
            "where": "cwhs_code ilike '{warehouse}' and (cgre_created_on > current_date -182) and (cdcs_name ilike 'Cerrado') and cgre_movement_type = '101'"
        }
    },
    {
        "name": "goods_issues",
        "params": {
            "orderby": "cgis_created_on desc",
            "take": 18000,
            "where": "cwhs_code ilike '{warehouse}' and (cgis_created_on > current_date -182) and (cdcs_name ilike 'Cerrado') and cgis_movement_type = '261'"
        }
    },
    {
        "name": "inbound_deliveries",
        "params": {
            "orderby": "cdoc_date desc",
            "take": 3000,
            "where": "cwhs_code ilike '{warehouse}' and (cdoc_date > current_date -182)"
        }
    },
    {
        "name": "outbound_deliveries",  # Nueva consulta
        "params": {
            "orderby": "codv_created_on desc",
            "take": 20000,
            "where": "cwhs_code ilike '{warehouse}' and (codv_created_on > current_date -182)"
        }
    }
]

# ======================
# FUNCIONES PRINCIPALES
# ======================
def build_url(endpoint, params, warehouse):
    """Construye URL con parámetros codificados"""
    encoded_params = []
    for key, value in params.items():
        if key == "where":
            value = value.format(warehouse=warehouse)
        encoded_params.append(f"{quote(key)}={quote(value)}")
    return f"{BASE_URL}{endpoint}?{'&'.join(encoded_params)}"

def fetch_api_data(url, query_name):
    """Obtiene datos con manejo de errores y reintentos"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"\033[36mℹ️  Consultando {query_name} (Intento {attempt + 1})\033[0m")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            if not data:
                print(f"\033[33m⚠️  {query_name} devolvió datos vacíos\033[0m")
                return None
                
            df = pd.json_normalize(data)
            df['load_timestamp'] = datetime.now().isoformat()
            df['query_name'] = query_name
            print(f"\033[32m✅ {query_name} - {len(df)} registros\033[0m")
            return df
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                print(f"\033[31m❌ {query_name} falló después de {MAX_RETRIES} reintentos: {str(e)}\033[0m")
                return None
            print(f"\033[35m⚡ Esperando {RETRY_DELAY}s para reintentar...\033[0m")
            time.sleep(RETRY_DELAY)

def process_warehouse(warehouse_code):
    """Procesa todas las consultas para un almacén"""
    print(f"\n\033[1m🔍 PROCESANDO ALMACÉN {warehouse_code}\033[0m")
    warehouse_data = {}
    
    for config in QUERY_CONFIG:
        url = build_url(ENDPOINTS[config["name"]], config["params"], warehouse_code)
        df = fetch_api_data(url, config["name"])
        
        if df is not None:
            warehouse_data[config["name"]] = df
        
        if config != QUERY_CONFIG[-1]:
            print(f"\033[34m⏳ Esperando {REQUEST_DELAY}s entre consultas...\033[0m")
            time.sleep(REQUEST_DELAY)
    
    return warehouse_data

def save_data(data, warehouse_code):
    """Guarda los DataFrames en archivos Parquet separados"""
    if not data:
        print(f"\033[31m❌ No hay datos para guardar del almacén {warehouse_code}\033[0m")
        return False
    
    os.makedirs("data", exist_ok=True)
    success = True
    
    for name, df in data.items():
        filename = f"data/{name}_{warehouse_code}.parquet"
        try:
            df.to_parquet(filename, index=False)
            size_mb = os.path.getsize(filename) / (1024 * 1024)
            print(f"\033[32m💾 {filename} - {len(df)} registros ({size_mb:.2f} MB)\033[0m")
        except Exception as e:
            print(f"\033[31m❌ Error guardando {filename}: {str(e)}\033[0m")
            success = False
    
    return success

# ======================
# EJECUCIÓN PRINCIPAL
# ======================
def main():
    """Función principal con reporting mejorado"""
    print("\n\033[1m🚀 INICIANDO RECOLECTOR DE DATOS\033[0m")
    start_time = time.time()
    
    try:
        for warehouse in WAREHOUSE_CODES:
            warehouse_data = process_warehouse(warehouse)
            save_data(warehouse_data, warehouse)
            
    except Exception as e:
        print(f"\033[31m💥 ERROR CRÍTICO: {str(e)}\033[0m")
        raise
    
    finally:
        duration = time.time() - start_time
        print(f"\n\033[1m⌛ PROCESO COMPLETADO EN {duration:.2f} SEGUNDOS\033[0m")

if __name__ == "__main__":
    main()
