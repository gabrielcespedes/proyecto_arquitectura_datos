SELECT * FROM
ML.PREDICT(
  MODEL `proyectosarquitecturabsg.dataset_analisis.modelo_maquina`,
  (SELECT * FROM `proyectosarquitecturabsg.dataset_analisis.gcs_bq`)
);