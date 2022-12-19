WITH Flattened AS (
  SELECT DISTINCT name_sender, date_transaction, id_payment_type, id_recipient, is_fraud, is_flagged_fraud
  FROM {{ ref('fact__tables') }}
  WHERE is_fraud = "Fraud" OR is_flagged_fraud = "Fraud"
  GROUP BY 1, 2, 3, 4, 5, 6
  ORDER BY name_sender
)

SELECT name_sender,
       array_agg(date_transaction) AS date_transaction,
       array_agg(id_payment_type) AS id_payment_type,
       array_agg(id_recipient) AS id_recipient,
       array_agg(is_fraud) AS is_fraud,
       array_agg(is_flagged_fraud) AS is_flagged_fraud,
FROM Flattened GROUP BY name_sender
ORDER BY name_sender

 