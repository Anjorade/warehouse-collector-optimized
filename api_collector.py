import os
import requests
import pandas as pd
import time
from datetime import datetime
from urllib.parse import quote

# ======================================
# CONFIGURACIÓN (SEGURA CON VARIABLES DE ENTORNO)
# ======================================
TOKEN = os.getenv("API_TOKEN")  # Desde GitHub Secrets
BASE_URL = os.getenv("API_BASE_URL")  # Desde GitHub Secrets
HEADERS = {"token": TOKEN}
WAREHOUSE_CODES = ['1145', '1290']  # Almacenes a procesar

# Configuración de comportamiento
MAX_RETRIES = 2           # Reintentos por consulta fallida
REQUEST_DELAY = 30        # Espera entre consultas (segundos)
RETRY_DELAY = 10          # Espera entre reintentos (segundos)

# ======================================
# DEFINICIÓN DE ENDPOINTS
# ======================================
ENDPOINTS = {
    "sales_orders": "/Ardisa.SalesOrders.List.View1",
    "goods_receipts": "/System.GoodsRecipts.List.View1",
    "goods_issues": "/System.GoodsIssues.List.View1",
    "inbound_deliveries": "/Ardisa.InboundDeliveries.List.View1",
    "outbound_deliveries": "/System.OutboundDeliveries.List.View1"  # Nuevo endpoint
}

# ======================================
# CONFIGURACIÓN DE CONSULTAS (TODOS LOS VALORES COMO STRINGS)
# ======================================
QUERY_CONFIG = [
    {
        "name": "sales_orders",
        "params": {
            "orderby": "cslo_created_on desc",
            "take": "20000",  # Convertido a string
            "where": "cwhs_code ilike '{warehouse}' and (cslo_created_on > current_date -182)"
        }
    },
    {
        "name": "goods_receipts",
        "params": {
            "orderby": "cgre_created_on desc",
            "take": "4000",
            "where": "cwhs_code ilike '{warehouse}' and (cgre_created_on > current_date -182) and (cdcs_name ilike 'Cerrado') and cgre_movement_type = '101'"
        }
    },
    {
        "name": "goods_issues",
        "params": {
            "orderby": "cgis_created_on desc",
            "take": "18000",
            "where": "cwhs_code ilike '{warehouse}' and (cgis_created_on > current_date -182) and (cdcs_name ilike 'Cerrado') and cgis_movement_type = '261'"
        }
    },
    {
        "name": "inbound_deliveries",
        "params": {
            "orderby": "cdoc_date desc",
            "take": "3000",
            "where": "cwhs_code ilike '{warehouse}' and (cdoc_date > current_date -182)"
        }
    },
    {
        "name": "outbound_deliveries",  # Nueva consulta
        "params": {
            "orderby": "codv_created_on desc",
            "take": "20000",
            "where": "cwhs_code ilike '{warehouse}' and (codv_created_on > current_date -182)"
        }
    }
]

# ======================================
# FUNCIONES PRINCIPALES (CORREGIDAS)
# ======================================
def build_url(endpoint, params, warehouse):
    """Construye URL con codificación segura para todos los parámetros"""
    encoded_params = []
    for key, value in params.items():
        # Asegura que todos los valores sean strings
        str_key = str(key)
        str_value = str(value.format(warehouse=warehouse) if key == "where" else value)
        
        # Codificación URL segura
        encoded_key = quote(str_key)
        encoded_value = quote(str_value)
        
        encoded_params.append(f"{encoded_key}={encoded_value}")
    
    return f"{BASE_URL}{endpoint}?{'&'.join(encoded_params)}"

def fetch_api_data(url, query_name):
    """Obtiene datos con manejo robusto de errores"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            print(f"\nℹ️  Consultando {query_name} (Intento {attempt + 1}/{MAX_RETRIES + 1})")
            response = requests.get(url, headers=HEADERS, timeout=60)
            response.raise_for_status()  # Lanza error para códigos 4XX/5XX
            
            data = response.json()
            if not data:
                print(f"⚠️  {query_name} devolvió datos vacíos")
                return None
                
            df = pd.json_normalize(data)
            df['load_timestamp'] = datetime.now().isoformat()
            df['query_name'] = query_name
            print(f"✅ {query_name} - {len(df)} registros obtenidos")
            return df
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                print(f"❌ {query_name} falló después de {MAX_RETRIES} reintentos: {str(e)}")
                return None
            print(f"⏳ Esperando {RETRY_DELAY}s antes de reintentar...")
            time.sleep(RETRY_DELAY)

def process_warehouse(warehouse_code):
    """Procesa todas las consultas para un almacén específico"""
    print(f"\n🔍 PROCESANDO ALMACÉN {warehouse_code}")
    warehouse_data = {}
    
    for config in QUERY_CONFIG:
        url = build_url(ENDPOINTS[config["name"]], config["params"], warehouse_code)
        df = fetch_api_data(url, config["name"])
        
        if df is not None:
            warehouse_data[config["name"]] = df
        
        if config != QUERY_CONFIG[-1]:  # No esperar después de la última consulta
            print(f"⏳ Pausa de {REQUEST_DELAY}s entre consultas...")
            time.sleep(REQUEST_DELAY)
    
    return warehouse_data

def save_data(data, warehouse_code):
    """Guarda los DataFrames en archivos Parquet separados"""
    if not data:
        print(f"❌ No hay datos para guardar del almacén {warehouse_code}")
        return False
    
    os.makedirs("data", exist_ok=True)
    success = True
    
    for name, df in data.items():
        filename = f"data/{name}_{warehouse_code}.parquet"
        try:
            df.to_parquet(filename, index=False)
            size_mb = os.path.getsize(filename) / (1024 * 1024)
            print(f"💾 {filename} - {len(df)} registros ({size_mb:.2f} MB)")
        except Exception as e:
            print(f"❌ Error guardando {filename}: {str(e)}")
            success = False
    
    return success

# ======================================
# EJECUCIÓN PRINCIPAL
# ======================================
def main():
    """Función principal con manejo estructurado de errores"""
    print("\n🚀 INICIANDO RECOLECTOR DE DATOS")
    start_time = time.time()
    
    try:
        for warehouse in WAREHOUSE_CODES:
            warehouse_data = process_warehouse(warehouse)
            save_data(warehouse_data, warehouse)
            
    except Exception as e:
        print(f"\n💥 ERROR CRÍTICO: {str(e)}")
        raise  # Propaga el error para que falle el workflow
    
    finally:
        duration = time.time() - start_time
        print(f"\n⌛ PROCESO COMPLETADO EN {duration:.2f} SEGUNDOS")

if __name__ == "__main__":
    main()
