CREATE OR REPLACE VIEW `vm-juanespavas.crypto_stream_ds.v_crypto_stream_flat` AS
SELECT
  -- verdadero tiempo del evento:
  SAFE.TIMESTAMP_MILLIS(CAST(p.raw.e AS INT64)) AS ts,
  p.symbol                                  AS symbol,
  CAST(p.close AS FLOAT64)                      AS close_24h,
  CAST(p.raw.volume   AS FLOAT64)               AS base_volume_24h,
  CAST(p.raw.quantity AS FLOAT64)               AS quote_volume_24h,
  -- opcionales
  CAST(p.raw.open AS FLOAT64) AS open_24h,
  CAST(p.raw.high AS FLOAT64) AS high_24h,
  CAST(p.raw.low  AS FLOAT64) AS low_24h
FROM `vm-juanespavas.crypto_stream_ds.crypto_stream` s
CROSS JOIN UNNEST(s.payload) AS p
WHERE p.symbol IS NOT NULL
  AND p.close IS NOT NULL;



CREATE OR REPLACE TABLE `vm-juanespavas.crypto_stream_ds.train_binance` AS
WITH base AS (
  SELECT * FROM `vm-juanespavas.crypto_stream_ds.v_crypto_stream_flat`
),
feat AS (
  SELECT
    ts, symbol, close_24h, base_volume_24h, quote_volume_24h,

    SAFE.LOG(SAFE_DIVIDE(
      close_24h,
      LAG(close_24h) OVER (PARTITION BY symbol ORDER BY ts)
    )) AS ret_5m_back,

    AVG(close_24h) OVER (
      PARTITION BY symbol ORDER BY ts
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS sma_3,
    AVG(close_24h) OVER (
      PARTITION BY symbol ORDER BY ts
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS sma_10,

    SAFE_DIVIDE(
      quote_volume_24h - LAG(quote_volume_24h) OVER (PARTITION BY symbol ORDER BY ts),
      NULLIF(LAG(quote_volume_24h) OVER (PARTITION BY symbol ORDER BY ts), 0)
    ) AS vol_chg,

    SAFE.LOG(SAFE_DIVIDE(
      LEAD(close_24h) OVER (PARTITION BY symbol ORDER BY ts),
      close_24h
    )) AS ret_5m_future
  FROM base
)
SELECT
  ts, symbol, close_24h, base_volume_24h, quote_volume_24h,
  ret_5m_back, sma_3, sma_10, vol_chg,
  CASE
    WHEN ret_5m_future >  0.0002 THEN 'SUBE'
    WHEN ret_5m_future < -0.0002 THEN 'BAJA'
    ELSE NULL
  END AS target
FROM feat
WHERE ret_5m_back   IS NOT NULL
  AND sma_3         IS NOT NULL
  AND sma_10        IS NOT NULL
  AND vol_chg       IS NOT NULL
  AND ret_5m_future IS NOT NULL
  AND (ret_5m_future > 0.0002 OR ret_5m_future < -0.0002);




CREATE OR REPLACE TABLE `vm-juanespavas.crypto_stream_ds.infer_features` AS
WITH base AS (
  SELECT * FROM `vm-juanespavas.crypto_stream_ds.v_crypto_stream_flat`
  WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
),
feat AS (
  SELECT
    ts, symbol, close_24h, base_volume_24h, quote_volume_24h,

    SAFE.LOG(SAFE_DIVIDE(
      close_24h, LAG(close_24h) OVER (PARTITION BY symbol ORDER BY ts)
    )) AS ret_5m_back,

    AVG(close_24h) OVER (
      PARTITION BY symbol ORDER BY ts
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS sma_3,
    AVG(close_24h) OVER (
      PARTITION BY symbol ORDER BY ts
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS sma_10,

    SAFE_DIVIDE(
      quote_volume_24h - LAG(quote_volume_24h) OVER (PARTITION BY symbol ORDER BY ts),
      NULLIF(LAG(quote_volume_24h) OVER (PARTITION BY symbol ORDER BY ts), 0)
    ) AS vol_chg
  FROM base
)
SELECT *
FROM feat
WHERE ret_5m_back IS NOT NULL
  AND sma_3 IS NOT NULL
  AND sma_10 IS NOT NULL
  AND vol_chg IS NOT NULL;