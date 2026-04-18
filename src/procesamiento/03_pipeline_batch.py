import os
import json
import csv
import re
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Configuración
RUTA_CREDENCIALES = "../../config/credenciales_arquitectura.json"
NOMBRE_BUCKET = "data_lake_bsg_2026" 

# Ruta exacta del archivo en el Data Lake 
RUTA_GCS_ENTRADA = f"gs://{NOMBRE_BUCKET}/datos_crudos/mantenimiento_historico.csv"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

try:
    with open(RUTA_CREDENCIALES, "r") as archivo:
        PROJECT_ID = json.load(archivo)["project_id"]
except Exception as e:
    print(f"Error al leer credenciales: {e}")
    exit()

# Usamos dataset de BigQuery
TABLA_DESTINO = f"{PROJECT_ID}:dataset_analisis.gcs_bq"

# Transformación
class LimpiezaSimple(beam.DoFn):
    def process(self, element):
        # Manejo de CSV robusto
        reader = csv.reader([element])
        try:
            valores = next(reader)
        except:
            return

        # Filtro de cabecera: si la primera columna no es un número, es la cabecera
        if not valores[0].isdigit():
            return

        # Funciones seguras de conversión
        def a_flotante(dato):
            try: return float(dato)
            except: return 0.0

        def a_entero(dato):
            try: return int(dato)
            except: return 0

        yield {
            "UDI": a_entero(valores[0]),
            "Product_ID": valores[1],
            "Type": valores[2],
            "Air_temperature__K_": a_flotante(valores[3]),
            "Process_temperature__K_": a_flotante(valores[4]),
            "Rotational_speed__rpm_": a_entero(valores[5]),
            "Torque__Nm_": a_flotante(valores[6]),
            "Tool_wear__min_": a_entero(valores[7]),
            "Target": a_entero(valores[8]),
            "Failure_Type": valores[9],
            "fecha_procesamiento_batch": datetime.now(timezone.utc).isoformat()
        }

# pipeline: GCS ==> BIGQUERY
def ejecutar():
    print(f"--- Iniciando Pipeline Batch GCS -> BigQuery ---")
    print(f"Origen: {RUTA_GCS_ENTRADA}")
    print(f"Destino: {TABLA_DESTINO}")
    
    opciones = PipelineOptions()
    
    with beam.Pipeline(options=opciones) as p:
        (
            p 
            | "Leer desde GCS" >> beam.io.ReadFromText(RUTA_GCS_ENTRADA, skip_header_lines=1)
            | "Transformar Tipos" >> beam.ParDo(LimpiezaSimple())
            | "Cargar a BigQuery" >> beam.io.WriteToBigQuery(
                TABLA_DESTINO,
                schema=(
                    "UDI:INTEGER, Product_ID:STRING, Type:STRING, "
                    "Air_temperature__K_:FLOAT, Process_temperature__K_:FLOAT, "
                    "Rotational_speed__rpm_:INTEGER, Torque__Nm_:FLOAT, "
                    "Tool_wear__min_:INTEGER, Target:INTEGER, Failure_Type:STRING, "
                    "fecha_procesamiento_batch:TIMESTAMP"
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://{NOMBRE_BUCKET}/temp"
            )
        )
    
    print("\n¡PIPELINE FINALIZADO CON ÉXITO!")
    print(f"Verifica la tabla '{TABLA_DESTINO}' en tu consola de GCP.")

if __name__ == "__main__":
    ejecutar()