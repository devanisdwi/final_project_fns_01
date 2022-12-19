WITH Flattened AS (
  SELECT DISTINCT name_recipient, date_transaction, id_payment_type, id_sender, is_fraud, is_flagged_fraud
  FROM {{ ref('fact__tables') }}
  WHERE is_fraud = "Fraud" OR is_flagged_fraud = "Fraud"
  GROUP BY 1, 2, 3, 4, 5, 6
  ORDER BY name_recipient
)

SELECT name_recipient,
       array_agg(date_transaction) AS date_transaction,
       array_agg(id_payment_type) AS id_payment_type,
       array_agg(id_sender) AS id_sender,
       array_agg(is_fraud) AS is_fraud,
       array_agg(is_flagged_fraud) AS is_flagged_fraud,
FROM Flattened GROUP BY name_recipient
ORDER BY name_recipient

 