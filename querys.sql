-- prueba basica
SELECT *
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`
LIMIT 5;



-- query sobre los ultimos 10 minutos
SELECT
  TIMESTAMP_MILLIS(t_ms) AS ts,
  p.symbol AS symbol,
  CAST(p.close AS FLOAT64) AS close_price
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`,
UNNEST(payload) AS p
WHERE TIMESTAMP_MILLIS(t_ms) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
ORDER BY t_ms DESC
LIMIT 50;
-- version sin ult minutos
SELECT
  TIMESTAMP_MILLIS(t_ms) AS ts,
  p.symbol AS symbol,
  CAST(p.close AS FLOAT64) AS close_price
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`,
UNNEST(payload) AS p
ORDER BY t_ms DESC
LIMIT 50;


-- query sobre en ult 30 min
---- (por ejemplo, evolución del precio promedio de cada moneda en los últimos 30 minutos).
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(t_ms), MINUTE) AS time_bucket,
  p.symbol AS symbol,
  AVG(CAST(p.close AS FLOAT64)) AS avg_price
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`,
UNNEST(payload) AS p
WHERE TIMESTAMP_MILLIS(t_ms)
  BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
  AND CURRENT_TIMESTAMP()
GROUP BY time_bucket, symbol
ORDER BY time_bucket ASC
-- version sin ult minutos
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(t_ms), MINUTE) AS time_bucket,
  p.symbol AS symbol,
  AVG(CAST(p.close AS FLOAT64)) AS avg_price
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`,
UNNEST(payload) AS p
GROUP BY time_bucket, symbol
ORDER BY time_bucket ASC;

-- ex 1 $ datos

SELECT
  TIMESTAMP_MILLIS(t_ms) AS ts,
  p.symbol AS symbol,
  CAST(p.close AS FLOAT64) AS close_price,
  CAST(p.raw.open AS FLOAT64) AS open_price,
  CAST(p.raw.high AS FLOAT64) AS high_price,
  CAST(p.raw.low AS FLOAT64) AS low_price,
  CAST(p.raw.volume AS FLOAT64) AS volume
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`,
UNNEST(payload) AS p
WHERE p.symbol = 'BTCUSDT'
ORDER BY t_ms DESC
LIMIT 100;


-- moneda promediada
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(t_ms), MINUTE) AS time_bucket,
  p.symbol AS symbol,
  AVG(CAST(p.close AS FLOAT64)) AS avg_close_price,
  AVG(CAST(p.raw.volume AS FLOAT64)) AS avg_volume
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream`,
UNNEST(payload) AS p
WHERE p.symbol = 'BTCUSDT'
GROUP BY time_bucket, symbol
ORDER BY time_bucket ASC;
