TRUNCATE TABLE `bitcoin-data-analysis-320014.transaction_analysis.blocks_with_mempool_2022_v4`;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-01-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight  
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;


CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-02-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;


CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-03-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;


CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-04-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-05-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-06-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-07-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-08-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-09-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value 
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m 
         ON m.first_seen_timestamp <= b.block_timestamp  
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-10-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks 
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value 
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m
         ON m.first_seen_timestamp <= b.block_timestamp
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-11-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m
         ON m.first_seen_timestamp <= b.block_timestamp
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;

CREATE OR REPLACE TEMPORARY TABLE params AS
SELECT DATETIME "2022-12-01" AS month
;

CREATE OR REPLACE TEMPORARY TABLE tmp_blocks
PARTITION BY DATE(block_timestamp) cluster by block_number
AS
SELECT block_number        AS block_number
     , block_timestamp_adj AS block_timestamp
     , block_txs           AS block_txs
     , difficulty_bits     AS difficulty_bits
     , block_weight        AS block_weight
     , block_value
     , block_fees
     , block_vbytes
  FROM `bitcoin-data-analysis-320014.transaction_analysis.block_cache_v4`
 WHERE 1=1
   AND DATETIME(block_timestamp_adj) >= (SELECT month FROM params)
   AND DATETIME(block_timestamp_adj) < DATE_ADD((SELECT month FROM params), interval 1 MONTH)
;

CREATE OR REPLACE TEMPORARY TABLE tmp_txs
PARTITION BY DATE(first_seen_timestamp) CLUSTER BY block_number
AS
SELECT * FROM (
       SELECT size
            , virtual_size
	    , fee
	    , output_value
            , GREATEST(first_seen_timestamp, TIMESTAMP_SUB(block_timestamp, INTERVAL 14 DAY)) AS first_seen_timestamp
	    , block_number
	    , block_timestamp
	    , tx_hash
         FROM `bitcoin-data-analysis-320014.transaction_analysis.mempool_txs_all_sources_v4`
	WHERE 1=1
          AND block_number IS NOT NULL
          AND DATETIME(block_timestamp) >= (SELECT month FROM params)
	  AND rebroadcast = 1
	)
        WHERE 1=1
          AND DATETIME(first_seen_timestamp) < DATE_ADD((SELECT month FROM params), INTERVAL 1 MONTH)
;

INSERT INTO transaction_analysis.blocks_with_mempool_2022_v4
SELECT block_number
     , block_timestamp
     , block_txs
     , block_vbytes
     , block_weight / 4000000    AS block_fullness
     , block_value
     , block_fees
     , pending_txs
     , pending_vbytes
     , pending_fees
     , pending_value
FROM (
SELECT b.block_number        AS block_number
     , b.block_timestamp     AS block_timestamp
     , b.block_txs           AS block_txs
     , b.difficulty_bits     AS difficulty_bits
     , b.block_weight        AS block_weight
     , b.block_value
     , b.block_fees
     , b.block_vbytes
     , COUNT(m.tx_hash)      AS pending_txs
     , SUM(m.size)           AS pending_size
     , SUM(m.virtual_size)   AS pending_vbytes
     , SUM(m.fee)            AS pending_fees
     , SUM(m.output_value)   AS pending_value
  FROM tmp_blocks b LEFT JOIN tmp_txs m
         ON m.first_seen_timestamp <= b.block_timestamp
        AND b.block_number < m.block_number
 WHERE 1=1

 GROUP BY 1,2,3,4,5,6,7,8)
;
