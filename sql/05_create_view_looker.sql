CREATE OR REPLACE VIEW `proyectosarquitecturabsg.dataset_streaming.v_sensores_limpia` AS
SELECT 
  document_id,
  timestamp AS hora_evento,
  -- Usamos COALESCE por si el UDI trae el caracter invisible (BOM) desde el archivo CSV original
  COALESCE(JSON_VALUE(data, '$."UDI"'), JSON_VALUE(data, '$."UDI"')) AS udi,
  JSON_VALUE(data, '$."Product ID"') AS product_id,
  JSON_VALUE(data, '$."Type"') AS tipo_maquina,
  SAFE_CAST(JSON_VALUE(data, '$."Air temperature [K]"') AS FLOAT64) AS temp_aire,
  SAFE_CAST(JSON_VALUE(data, '$."Process temperature [K]"') AS FLOAT64) AS temp_proceso,
  SAFE_CAST(JSON_VALUE(data, '$."Rotational speed [rpm]"') AS FLOAT64) AS rpm,
  SAFE_CAST(JSON_VALUE(data, '$."Torque [Nm]"') AS FLOAT64) AS torque,
  SAFE_CAST(JSON_VALUE(data, '$."Tool wear [min]"') AS FLOAT64) AS desgaste_herramienta,
  JSON_VALUE(data, '$."Failure Type"') AS tipo_falla
FROM `proyectosarquitecturabsg.dataset_streaming.sensores_tiempo_real_raw_latest`
