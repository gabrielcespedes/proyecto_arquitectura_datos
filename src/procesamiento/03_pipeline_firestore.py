import os
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import firestore

# 1. CONFIGURACIÓN
RUTA_CREDENCIALES = "../../config/credenciales_arquitectura.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

with open(RUTA_CREDENCIALES, "r") as f:
    PROJECT_ID = json.load(f)["project_id"]

TOPIC_ID = "datos_streaming_bsg" # Tu Topic actual
COLECCION_DESTINO = "monitoreo_sensores"

# 2. TRANSFORMACIÓN: Escritura en Firestore
class EscribirEnFirestore(beam.DoFn):
    def process(self, element):
        db = firestore.Client(project=PROJECT_ID, database = "basefirestore")
        
        # TRUCO: Buscar la llave real que contiene 'UDI' ignorando caracteres invisibles
        llave_udi = next((k for k in element.keys() if 'UDI' in k), None)
        
        if llave_udi:
            valor_udi = str(element[llave_udi])
            doc_ref = db.collection(COLECCION_DESTINO).document(valor_udi)
            doc_ref.set(element, merge=True)
            
            # En vez de pasar todo el diccionario, pasamos solo el ID limpio para imprimirlo
            yield valor_udi 
        else:
            print("Mensaje ignorado (Sin UDI válido)")

# 3. PIPELINE
def ejecutar_streaming():
    opciones = PipelineOptions(streaming=True)
    
    print("Pipeline escuchando a Pub/Sub... (Presiona Ctrl+C para detener)")
    
    with beam.Pipeline(options=opciones) as p:
        (
            p 
            | "Leer de PubSub" >> beam.io.ReadFromPubSub(topic=f"projects/{PROJECT_ID}/topics/{TOPIC_ID}")
            | "Decodificar JSON" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
            | "Escribir en Firestore" >> beam.ParDo(EscribirEnFirestore())
            # Ahora x es simplemente el valor_udi (ej: "1", "2", "3")
            | "Log en Consola" >> beam.Map(lambda x: print(f"Firestore actualizado: Sensor {x}"))
        )

if __name__ == "__main__":
    ejecutar_streaming()