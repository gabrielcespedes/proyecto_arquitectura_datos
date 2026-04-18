import os
import json
import csv
import re
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
RUTA_CREDENCIALES = "../../config/credenciales_arquitectura.json"
ARCHIVO_LOCAL = "../../data/predictive_maintenance.csv"
NOMBRE_BUCKET = "data_lake_bsg_2026" 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = RUTA_CREDENCIALES

try:
    with open(RUTA_CREDENCIALES, "r") as archivo:
        PROJECT_ID = json.load(archivo)["project_id"]
except Exception as e:
    print(f"Error al leer credenciales: {e}")
    exit()

# Usamos una tabla de respaldo, en el conjunto de datos de BigQuery
TABLA_DESTINO = f"{PROJECT_ID}:dataset_analisis.local_bq"

# Lógica de transformación
class LimpiezaSimple(beam.DoFn):
    def process(self, element):
        reader = csv.reader([element])
        valores = next(reader)
        
        # Filtro de seguridad para cabeceras
        if valores[0] == "UDI":
            return

        # Funciones seguras para conversión de tipos
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
            # CAMBIO SOLICITADO: Formato ISO estándar para TIMESTAMP
            "fecha_procesamiento_batch": datetime.now(timezone.utc).isoformat()
        }

# Ejecución del Pipeline
def ejecutar():
    print("Iniciando carga Batch final con tipos numéricos...")
    
    opciones = PipelineOptions()
    
    with beam.Pipeline(options=opciones) as p:
        (
            p 
            | "Leer Local" >> beam.io.ReadFromText(ARCHIVO_LOCAL, skip_header_lines=1)
            | "Transformar Tipos" >> beam.ParDo(LimpiezaSimple())
            | "Cargar a BigQuery" >> beam.io.WriteToBigQuery(
                TABLA_DESTINO,
                schema=(
                    "UDI:INTEGER, "
                    "Product_ID:STRING, "
                    "Type:STRING, "
                    "Air_temperature__K_:FLOAT, "
                    "Process_temperature__K_:FLOAT, "
                    "Rotational_speed__rpm_:INTEGER, "
                    "Torque__Nm_:FLOAT, "
                    "Tool_wear__min_:INTEGER, "
                    "Target:INTEGER, "
                    "Failure_Type:STRING, "
                    "fecha_procesamiento_batch:TIMESTAMP"
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://{NOMBRE_BUCKET}/temp"
            )
        )
    print("Proceso completado.")

if __name__ == "__main__":
    ejecutar()