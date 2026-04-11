import os

import json

from google.cloud import storage # cliente storage

# 1. apuntar la llave JSON descargada
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credenciales_arquitectura.json"

# 2. ID del proyecto
with open("credenciales_arquitectura.json", "r") as archivo_json:
    PROJECT_ID = json.load(archivo_json)["project_id"]

print("Prueba de entorno local.")

try:
    # instanciar el cliente, al no pasar credenciales explícitas, buscará el archivo referenciado en paso 1.
    storage_client = storage.Client(project = PROJECT_ID)

    # petición gratuita: listar buckets
    buckets = list(storage_client.list_buckets())

    print("Conexión exitosa.")

    # impresión de buckets
    print("Buckets en Google Cloud Storage.")
    if len(buckets) == 0:
        print("No hay buckets por el momento.")
    else:
        print(f"Se encontraron {len(buckets)} buckets:")
        for i, bucket in enumerate(buckets, start = 1):
            print(f"{i}. {bucket.name}")
except Exception as e:
    print(f"Error de conexión en GCP. Verifique su JSON y su PROJECT_ID. Detalle: {e}")
