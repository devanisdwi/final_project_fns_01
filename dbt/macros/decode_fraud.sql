{% macro decode_isFraud(isFraud) -%}
    case {{ isFraud }}
        when 0 then "Not Fraud"
        when 1 then "Fraud"
    end
{%- endmacro %}

{% macro decode_isFlaggedFraud(isFlaggedFraud) -%}
    case {{ isFlaggedFraud }}
        when 0 then "Not Fraud"
        when 1 then "Fraud"
    end
{%- endmacro %}



{% macro add_id_payment(type, id_payment_type) -%}
    case {{ type }}
        when {{ type }} "CASH_IN" THEN 1
        when {{ type }} "CASH_OUT" THEN 2
        when {{ type }} "DEBIT" THEN 3
        when {{ type }} "PAYMENT" THEN 4
    end as id_payment_type
{%- endmacro %}
