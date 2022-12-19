with fact__tables as (
    SELECT * 
    FROM {{ ref('stg__fraud') }}
)

SELECT 
    fact__tables.id_transaction,
    fact__tables.step,
    fact__tables.date_transaction,
    fact__tables.amount,
    fact__tables.name_sender,
    fact__tables.old_balance_sender,
    fact__tables.new_balance_sender,
    fact__tables.name_recipient,
    fact__tables.old_balance_recipient,
    fact__tables.new_balance_recipient,
    fact__tables.is_fraud,
    fact__tables.is_flagged_fraud,
    pt.id_payment_type,
    sd.id_sender,
    rp.id_recipient,
FROM
    fact__tables
    INNER JOIN {{ ref('dim__payment_type') }} pt ON fact__tables.payment_type = pt.payment_type
    INNER JOIN {{ ref('dim__sender') }} sd ON fact__tables.name_sender = sd.name_sender
    INNER JOIN {{ ref('dim__recipient') }} rp ON fact__tables.name_recipient = rp.name_recipient