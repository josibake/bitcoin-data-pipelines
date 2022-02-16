UPDATE
  `bitcoin-data-analysis-320014.transaction_analysis.txs_two_or_less_outputs` txs
SET
  txs.heuristic = 'previously_seen_address',
  txs.payment_value = v.value,
  txs.change_value = txs.output_value - v.value
FROM (
  WITH
    outputs AS (
    SELECT
      transaction_hash,
      block_number,
      addr,
      value
    FROM
      `bigquery-public-data.crypto_bitcoin.outputs`,
      UNNEST(addresses) addr ),
    reused AS (
    SELECT
      a.transaction_hash,
      a.addr,
      a.value,
    FROM
      outputs a
    WHERE
      EXISTS (
      SELECT
        1
      FROM
        outputs b
      WHERE
        a.transaction_hash != b.transaction_hash
        AND a.block_number > b.block_number
        AND a.addr = b.addr ) )
  SELECT
    CAST(value AS INTEGER) value,
    transaction_hash,
  FROM (
    SELECT
      transaction_hash,
      value,
      COUNT(*) OVER(PARTITION BY transaction_hash) AS total,
    FROM
      reused)
  WHERE
    total = 1 ) v
WHERE
  txs.tx_hash = v.transaction_hash
  AND txs.heuristic IS NULL
