UPDATE `bitcoin-data-analysis-320014.transaction_analysis.txs_two_or_less_outputs` tx
  SET tx.payment_usd = ROUND((tx.payment_value * (p.Open + p.Close)/2)/100000000, 2),
      tx.change_usd = ROUND((tx.change_value * (p.Open + p.Close)/2)/100000000, 2),
      tx.fee_usd = ROUND((tx.mining_fee * (p.Open + p.Close)/2)/100000000, 2)
 FROM `bitcoin-data-analysis-320014.transaction_analysis.daily_btcusd_yahoo` p
WHERE date(tx.block_timestamp) = p.Date
