-- After being sure that we have created everything (dbt run), preview here and then validate the compiled code

{# in dbt Develop #}

{% set old_etl_relation=ref('customer_orders') -%}

{% set dbt_relation=ref('fct_customer__orders') %}

{{ audit_helper.compare_relations(
      a_relation=old_etl_relation,
      b_relation=dbt_relation,
      primary_key="order_id"
) }}