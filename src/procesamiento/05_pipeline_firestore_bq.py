import os
import json 
import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import firestore
from dotenv import load_dotenv

# 1. CONFIGURACIÓN
load_dotenv()

RUTA_CREDENCIALES = "../../config/credenciales_arquitectura.json"

# Autenticación mediante variable de entorno
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

with open(RUTA_CREDENCIALES, "r") as f:
    credenciales = json.load(f)
    PROJECT_ID = credenciales["project_id"]

# Variables de conexión
TOPIC_ID = "datos_streaming_bsg"
COLECCION_DESTINO = "monitoreo_sensores"

# 2. TRANSFORMACIÓN: Escritura en Firestore
class EscribirEnFirestore(beam.DoFn):
    def process(self, element):
        # Al no pasar el parámetro 'database', se conecta automáticamente a '(default)'
        # Esto coincide con la configuración que pusimos en la extensión de Firebase
        db = firestore.Client(project=PROJECT_ID, database="basefirestore")

        # Buscamos la llave del ID (UDI) de forma dinámica
        llave_udi = next((k for k in element.keys() if 'UDI' in k), None)
        
        if llave_udi:
            valor_udi = str(element[llave_udi])
            # Referencia: Colección -> Documento
            doc_ref = db.collection(COLECCION_DESTINO).document(valor_udi)
            
            # Upsert: Crea si no existe, actualiza si ya existe
            doc_ref.set(element, merge=True)
            
            yield valor_udi 
        else:
            print("Mensaje omitido: No se encontró un identificador válido (UDI)")

# 3. CONSTRUCCIÓN DEL PIPELINE
def ejecutar_streaming():
    # Modo streaming activado para procesar datos sin fin
    opciones = PipelineOptions(streaming=True)
    
    print(f"Antena iniciada. Escuchando tópico: {TOPIC_ID}")
    print("Los datos viajarán de Pub/Sub -> Firestore -> BigQuery automáticamente.")
    
    with beam.Pipeline(options=opciones) as p:
        (
            p 
            # Paso 1: Escuchar el buzón de Pub/Sub
            | "Leer de PubSub" >> beam.io.ReadFromPubSub(topic=f"projects/{PROJECT_ID}/topics/{TOPIC_ID}")

            # Paso 2: Convertir bytes a diccionario Python
            | "Decodificar JSON" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))

            # Paso 3: Guardar en Firestore (La extensión hará el resto hacia BigQuery)
            | "Escribir en Firestore" >> beam.ParDo(EscribirEnFirestore())

            # Paso 4: Confirmación visual en terminal
            | "Log en Consola" >> beam.Map(lambda x: print(f"Evento procesado: Sensor ID {x}"))
        )

if __name__ == "__main__":
    ejecutar_streaming()