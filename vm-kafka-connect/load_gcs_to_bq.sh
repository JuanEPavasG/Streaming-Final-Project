#!/bin/bash
# --------------------------------------------------------
# Script: load_gcs_to_bq.sh
# Propósito: Cargar archivos JSON desde GCS a BigQuery
# Dataset: crypto_stream_ds
# Tabla: crypto_stream
# --------------------------------------------------------

# Detener el script si hay algún error
set -e

# Archivo de log
LOGFILE=~/kafka-connect/connect-gcs-to-bq/load_gcs_to_bq.log

# Timestamp para logs
echo "==== Carga iniciada: $(date) ====" >> $LOGFILE

# Eliminar la tabla existente para poder crearla de nuevo
echo "==== Eliminando tabla previa (si existe) ====" >> $LOGFILE
bq rm -f crypto_stream_ds.crypto_stream >> $LOGFILE 2>&1 || true

# Comando de carga a BigQuery
# --replace=true: reemplaza la tabla existente si es necesario
# --autodetect: detecta el esquema automáticamente
# --source_format=NEWLINE_DELIMITED_JSON: formato esperado
echo "==== Cargando datos desde GCS a BigQuery ====" >> $LOGFILE
bq load --replace=true --autodetect --source_format=NEWLINE_DELIMITED_JSON \
  crypto_stream_ds.crypto_stream \
  gs://crypto-data-lake/raw/*.json >> $LOGFILE 2>&1

# Mensaje de éxito
echo "==== Carga finalizada: $(date) ====" >> $LOGFILE
echo "" >> $LOGFILE