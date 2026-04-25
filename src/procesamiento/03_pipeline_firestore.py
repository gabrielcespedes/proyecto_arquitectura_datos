import os
import json
import apache_beam as beam
# clase PipelineOptions: para seleccionar modo streaming para el motor de procesamiento
from apache_beam.options.pipeline_options import PipelineOptions
# firestore: SDK de Google para comunicarnos con FireStore
from google.cloud import firestore

from google.cloud import aiplatform

from dotenv import load_dotenv

load_dotenv()

# 1. CONFIGURACIÓN
RUTA_CREDENCIALES = "../../config/credenciales_arquitectura.json"

ENDPOINT_ID = os.environ.get("VERTEX_ENDPOINT_ID")

REGION = os.environ.get("VERTEX_REGION")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

with open(RUTA_CREDENCIALES, "r") as f:
    PROJECT_ID = json.load(f)["project_id"]

TOPIC_ID = "datos_streaming_bsg" # Tu Topic actual
COLECCION_DESTINO = "monitoreo_sensores"


class PredecirConVertexAI(beam.DoFn):
    
    def setup(self):
        # Conexión inicial a Vertex AI al arrancar el proceso
        aiplatform.init(project=PROJECT_ID, location=REGION)
        self.endpoint = aiplatform.Endpoint(ENDPOINT_ID)

    def process(self, element):
        # 1. Preparar las variables exactas que el modelo aprendió en SQL
        instancia = {
            "Air_temperature__K_": float(element.get("Air_temperature__K_", 0)),
            "Process_temperature__K_": float(element.get("Process_temperature__K_", 0)),
            "Rotational_speed__rpm_": float(element.get("Rotational_speed__rpm_", 0)),
            "Torque__Nm_": float(element.get("Torque__Nm_", 0)),
            "Tool_wear__min_": float(element.get("Tool_wear__min_", 0))
        }

        try:
            # 2. Enviar el dato al Endpoint y esperar la respuesta
            respuesta = self.endpoint.predict(instances=[instancia])
            
            # 3. Extraer la predicción
            prediccion = respuesta.predictions[0]
            
            # 4. Inyectar la predicción en nuestro diccionario original
            element["Prediccion_Falla"] = int(prediccion.get("predicted_Machine_failure", 0))
            
        except Exception as e:
            print(f"Error consultando al modelo: {e}")
            element["Prediccion_Falla"] = "Error_ML"

        # 5. Pasar el dato (ahora enriquecido) al siguiente paso
        yield element

# 2. TRANSFORMACIÓN: Escritura en Firestore
# Lógica del Worker
# hereda de beam.DoFn (Do Function: función de trabajo)
# se "avisa" a Apache Beam que es una clase trabajador que puede ser clonado (paralelización)
class EscribirEnFirestore(beam.DoFn):
    # método process: obligatorio en un DoFn. Se ejecuta una vez por cada elemento (mensaje)
    def process(self, element):
        # instanciamos la conexión a la base de datos
        # debe ser aquí dentro para que cada hilo de procesamiento tenga su conexión idepen.
        db = firestore.Client(project=PROJECT_ID, database = "basefirestore")
        
        # TRUCO: Buscar la llave real que contiene 'UDI' ignorando caracteres invisibles
        llave_udi = next((k for k in element.keys() if 'UDI' in k), None)
        
        if llave_udi:
            # Firestore requiere el ID del documento (nombre del archivo) debe ser string
            valor_udi = str(element[llave_udi])
            # ruta para guardar la información
            # base de datos -> colección -> documento (ej "1001")
            doc_ref = db.collection(COLECCION_DESTINO).document(valor_udi)
            # escritura, .set(): reemplaza todo el documento, al agregar
            # merge=True, se aplica un "Upsert" (si no existe se crea, si existe se reemplaza)
            doc_ref.set(element, merge=True)
            
            # En vez de pasar todo el diccionario, pasamos solo el ID limpio para imprimirlo
            yield valor_udi 
        else:
            print("Mensaje ignorado (Sin UDI válido)")

# 3. PIPELINE
def ejecutar_streaming():
    # creamos un objeto de opciones, parámetro streaming = True
    # procesamiento en tiempo real
    opciones = PipelineOptions(streaming=True)
    
    print("Pipeline escuchando a Pub/Sub... (Presiona Ctrl+C para detener)")
    
    # instanciamos el Pipeline con las opciones
    # with: asegura que cuando se detenga el programa se libere la RAM usada.
    with beam.Pipeline(options=opciones) as p:
        (
            # p: punto de inicio del pipeline
            p 

            # leemos desde la ruta completa del Topic
            | "Leer de PubSub" >> beam.io.ReadFromPubSub(topic=f"projects/{PROJECT_ID}/topics/{TOPIC_ID}")

            # beam.Map: aplica una función lambda a acada mensaje
            # mensajes de Pub/Sub llegan en formato Bytes (x)
            # .decode("utf-8"): transforma a texto normal
            # json.loads() -> convierte a diccionario Python
            | "Decodificar JSON" >> beam.Map(lambda x: json.loads(x.decode("utf-8")))

            | "Inferencia ML" >> beam.ParDo(PredecirConVertexAI())

            # beam.ParDo (Parallel Do) llama a clase 'EscribirEnFirestore'
            # se aplica 'yield valor_udi'
            | "Escribir en Firestore" >> beam.ParDo(EscribirEnFirestore())

            # Ahora x es simplemente el valor_udi (ej: "1", "2", "3")
            | "Log en Consola" >> beam.Map(lambda x: print(f"Firestore actualizado: Sensor {x}"))
        )

if __name__ == "__main__":
    ejecutar_streaming()