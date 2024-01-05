-- this is the DDL to create the table, but its combined here as a create or replace
-- for convenience. ideally, we have a separate DDL table query and then use the select
-- statement to insert data into the table
CREATE OR REPLACE TABLE mempool_data.mempool_transactions_dev (
txid STRING,
first_seen_timestamp TIMESTAMP,
last_seen_timestamp TIMESTAMP,
block_timestamp TIMESTAMP,
block_height INTEGER,
size INTEGER,
virtual_size INTEGER,
output_value INTEGER,
fee INTEGER
) PARTITION BY DATE(first_seen_timestamp)
AS
(
SELECT m.txid
     , m.first_seen_timestamp
     , m.last_seen_timestamp
     , t.block_timestamp
     , t.block_number AS block_height
     , t.size
     , t.virtual_size
     , CAST(t.output_value AS INTEGER) output_value
     , CAST(t.fee AS INTEGER) fee
  FROM (
    SELECT txhash AS txid
         , MIN(node_timestamp) AS first_seen_timestamp
         , MAX(node_timestamp) AS last_seen_timestamp
      FROM (
        SELECT txhash, TIMESTAMP_MICROS(`timestamp`) AS node_timestamp
          FROM `bitcoin-data-analysis-320014.mempool_data.mempool_events_dev` e
         WHERE e.source = "bmon"
           -- AND dt NOT IN ('2022-12-07', '2023-02-28')
        )
     GROUP BY 1
 --    this section is meant to pull in transactions that were not mined at the time the query was ran
 --    but were then mined in between the last time the query was ran and now
 --    this is not working as it is written, which is why it is commented out
 --
 --
 --    UNION ALL
 --    SELECT txid
 --         , first_seen_timestamp
 --         , last_seen_timestamp
 --      FROM `bitcoin-data-analysis-320014.mempool_data.mempool_transactions_dev` prev
 --     WHERE prev.block_height IS NULL
  ) m
  LEFT JOIN `bigquery-public-data.crypto_bitcoin.transactions` t
    ON m.txid = t.hash)
