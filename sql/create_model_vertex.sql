CREATE OR REPLACE MODEL `proyecto_id.conjunto_datos_bq.nombre_modelo`
OPTIONS(
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['Target'],
  max_iterations = 10,
  -- envío a registro a Vertex AI
  model_registry = 'vertex_ai',
  vertex_ai_model_id = 'modelo_predictivo_sensores',
  vertex_ai_model_version_aliases = ['v1_clase4']
) AS
SELECT 
  Air_temperature__K_,
  Process_temperature__K_,
  Rotational_speed__rpm_,
  Torque__Nm_,
  Tool_wear__min_,
  Target
FROM `proyecto_id.conjunto_datos_bq.tabla_bq`;