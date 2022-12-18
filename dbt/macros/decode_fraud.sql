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