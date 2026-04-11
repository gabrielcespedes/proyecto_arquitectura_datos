import os
import json
from google.cloud import storage

# ==========================================
# 1. CONFIGURACIÓN DINÁMICA Y AUTENTICACIÓN
# ==========================================
RUTA_CREDENCIALES = "credenciales_arquitectura.json"

# Apuntar la variable de entorno para GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

# Extraemos el ID leyendo directamente la llave
try:
    with open(RUTA_CREDENCIALES, "r") as archivo:
        datos_llave = json.load(archivo)
        PROJECT_ID = datos_llave["project_id"]
        print(f"Proyecto detectado automáticamente: {PROJECT_ID}")
except FileNotFoundError:
    print(f"ERROR: No se encontró el archivo '{RUTA_CREDENCIALES}'.")
    exit() # Detiene el script si no hay llave

# ==========================================
# 2. VARIABLES DE INGESTA
# ==========================================
# IMPORTANTE: Reemplace esto por el nombre único del bucket que creó en la consola
NOMBRE_BUCKET = "data_lake_bsg_2026" 
ARCHIVO_LOCAL = "data/predictive_maintenance.csv"
RUTA_DESTINO_NUBE = "datos_crudos/mantenimiento_historico.csv"

# ==========================================
# 3. FUNCIÓN DE CARGA A STORAGE
# ==========================================
def subir_a_storage(bucket_name, source_file_name, destination_blob_name):
    try:
        # El cliente ahora usa el ID dinámico
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        print(f"Subiendo '{source_file_name}' a gs://{bucket_name}/{destination_blob_name}...")
        
        blob.upload_from_filename(source_file_name)
        
        print("¡ÉXITO! Ingesta Batch completada.")
        print("El archivo ya está en el Data Lake (Cloud Storage).")

    except Exception as e:
        print(f"Error durante la subida: {e}")

# Ejecución
if __name__ == "__main__":
    print("\n--- Iniciando Proceso de Ingesta Batch ---")
    subir_a_storage(NOMBRE_BUCKET, ARCHIVO_LOCAL, RUTA_DESTINO_NUBE)