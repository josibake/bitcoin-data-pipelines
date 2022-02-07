UPDATE `bitcoin-data-analysis-320014.transaction_analysis.txs_two_or_less_outputs` tx
   SET tx.block_fullness = s.block_fullness 
  FROM (SELECT block_number, SUM(virtual_size)/1000000 AS block_fullness FROM `bigquery-public-data.crypto_bitcoin.transactions` GROUP BY 1 ) s 
WHERE tx.block_number = s.block_number
