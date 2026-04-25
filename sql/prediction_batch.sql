SELECT
  *
FROM
  ML.PREDICT(MODEL `proyecto_id.conjunto_datos_bq.nombre_modelo`,
    (
    SELECT
      Air_temperature__K_, 
      Process_temperature__K_, 
      Rotational_speed__rpm_, 
      Torque__Nm_, 
      Tool_wear__min_, 
      Target -- Se agrega el Target original aquí
    FROM
      `proyecto_id.conjunto_datos_bq.tabla_bq`
    LIMIT 20
    )
  );

