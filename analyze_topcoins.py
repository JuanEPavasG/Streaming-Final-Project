from google.cloud import bigquery
from datetime import datetime
import pandas as pd

PROJECT_ID = "vm-juanespavas"
DATASET = "crypto_stream_ds"
SOURCE_TABLE = f"{PROJECT_ID}.{DATASET}.crypto_stream"
TARGET_TABLE = f"{PROJECT_ID}.{DATASET}.rankings"

def main():
    client = bigquery.Client(project=PROJECT_ID)
    
    # Limpiar la tabla antes de insertar
    print("Limpiando la tabla de rankings...")
    client.query(f"TRUNCATE TABLE {TARGET_TABLE}").result()
    print("Tabla limpia.")

    # Consulta los últimos 10 minutos, usando SAFE_CAST y filtrando valores inválidos
    query = f"""
        SELECT
            payload.symbol AS symbol,
            AVG(CAST(payload.close AS FLOAT64)) AS avg_price,
            ARRAY_AGG(CAST(payload.close AS FLOAT64) ORDER BY TIMESTAMP_MILLIS(t_ms) DESC LIMIT 1)[OFFSET(0)] AS last_price,
            ARRAY_AGG(CAST(payload.close AS FLOAT64) ORDER BY TIMESTAMP_MILLIS(t_ms) ASC LIMIT 1)[OFFSET(0)] AS first_price,
            COUNT(*) AS n_events
        FROM `{SOURCE_TABLE}`
        CROSS JOIN UNNEST(payload) AS payload
        WHERE t_ms IS NOT NULL
            AND TIMESTAMP_MILLIS(t_ms) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
        GROUP BY symbol
    """

    df = client.query(query).to_dataframe()
    if df.empty:
        print("No hay datos recientes para analizar.")
        return

    df["pct_change"] = (df["last_price"] - df["first_price"]) / df["first_price"] * 100

    # Top 5 en subida y bajada
    top_up = df.sort_values("pct_change", ascending=False).head(5)
    top_down = df.sort_values("pct_change", ascending=True).head(5)

    timestamp = datetime.utcnow().isoformat()
    rows = []

    for rank, row in enumerate(top_up.itertuples(), start=1):
        rows.append({
            "symbol": row.symbol,
            "pct_change": float(row.pct_change),
            "direction": "up",
            "rank": rank,
            "window_ts": timestamp,
            "n_events": int(row.n_events)
        })
    for rank, row in enumerate(top_down.itertuples(), start=1):
        rows.append({
            "symbol": row.symbol,
            "pct_change": float(row.pct_change),
            "direction": "down",
            "rank": rank,
            "window_ts": timestamp,
            "n_events": int(row.n_events)
        })

    # Insertar en BigQuery
    errors = client.insert_rows_json(TARGET_TABLE, rows)
    if errors:
        print("Errores al insertar:", errors)
    else:
        print(f"Se insertaron {len(rows)} filas en {TARGET_TABLE}")

if __name__ == "__main__":
    main()