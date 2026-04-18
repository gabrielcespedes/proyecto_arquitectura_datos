import os
import json
import csv
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1 # pip install google-cloud-pubsub

# ==========================================
# 1. CONFIGURACIÓN DINÁMICA Y AUTENTICACIÓN
# ==========================================
RUTA_CREDENCIALES = "../../config/credenciales_arquitectura.json"
ARCHIVO_CSV = "../../data/predictive_maintenance.csv"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

try:
    with open(RUTA_CREDENCIALES, "r") as archivo:
        PROJECT_ID = json.load(archivo)["project_id"]
except FileNotFoundError:
    print(f"ERROR: No se encontró '{RUTA_CREDENCIALES}'.")
    exit()

# ==========================================
# 2. VARIABLES DE STREAMING
# ==========================================
# El ID del Tema (Topic)
TOPIC_ID = "datos_streaming_bsg" 

# ==========================================
# 3. LÓGICA DEL PRODUCTOR (EMISIÓN EN VIVO)
# ==========================================
def iniciar_streaming():
    # Instanciamos el cliente publicador de Pub/Sub
    publisher = pubsub_v1.PublisherClient()
    
    # Construimos la ruta oficial que exige Google Cloud
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Iniciando transmisión en vivo hacia: {topic_path}\n")
    print("Presiona Ctrl+C en cualquier momento para detener la máquina.\n")

    try:
        # Abrimos el archivo CSV
        with open(ARCHIVO_CSV, mode='r', encoding='utf-8') as archivo_datos:
            # DictReader convierte mágicamente cada fila en un Diccionario
            lector_csv = csv.DictReader(archivo_datos)
            
            contador = 0
            for fila in lector_csv:
                contador += 1
                
                # INYECCIÓN DE TIEMPO REAL: Agregamos el timestamp actual
                fila['event_timestamp'] = datetime.now(timezone.utc).isoformat() + "Z"
                
                # Pub/Sub solo acepta bytes (texto plano codificado)
                # Convertimos el diccionario a un string JSON, y luego a Bytes
                mensaje_json = json.dumps(fila)
                mensaje_bytes = mensaje_json.encode("utf-8")
                
                # Publicamos el mensaje en el Tópico
                future = publisher.publish(topic_path, mensaje_bytes)
                
                # Extraemos y mostramos un resumen en la terminal
                id_mensaje = future.result()

                # Extraemos dinámicamente los nombres de las dos primeras columnas del CSV
                columnas = list(fila.keys())
                col_1, col_2 = columnas[0], columnas[1]

                # Armamos un resumen universal
                resumen = f"{col_1}: {fila[col_1]} | {col_2}: {fila[col_2]}"

                # Extraemos solo la hora (HH:MM:SS) del timestamp para que se vea más limpio en consola
                hora_corta = fila['event_timestamp'][11:19]

                print(f"[{contador}] ID: {id_mensaje} | {resumen} | {hora_corta}")
                
                # EL SECRETO DEL STREAMING: Pausar el script
                # Si no pausamos, enviará las 10,000 filas en 2 segundos (eso sería Batch)
                time.sleep(1.5) # Pausa de 1.5 segundos entre cada mensaje

    except FileNotFoundError:
        print(f"Error: El archivo '{ARCHIVO_CSV}' no existe en esta carpeta.")
    except KeyboardInterrupt:
        print("\nTransmisión detenida por el usuario. Máquina apagada.")
    except Exception as e:
        print(f"\nError inesperado durante el streaming: {e}")

# Ejecución
if __name__ == "__main__":
    iniciar_streaming()