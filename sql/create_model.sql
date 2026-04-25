CREATE OR REPLACE MODEL `proyecto_id.conjunto_datos_bq.nombre_modelo`
OPTIONS(
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['Target'],
  max_iterations = 10,
  enable_global_explain = TRUE
) AS
SELECT 
  Air_temperature__K_,
  Process_temperature__K_,
  Rotational_speed__rpm_,
  Torque__Nm_,
  Tool_wear__min_,
  Target
FROM `proyecto_id.conjunto_datos_bq.tabla_bq`;