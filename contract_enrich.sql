{% macro contract_enrich(source_name, source_table_name, customer_snp_name, flg_portfolio_rule, contract_offers_table_name, except_cols=[]) %}
    WITH CUSTOMER AS (
        select
            customer_snp.CUSTOMER_ID as customer_snp_customer_id
            ,customer_snp.customer_version_id as customer_version_id
            ,ref_bu.BUSINESS_UNIT_LABEL as BUSINESS_UNIT_LABEL
            ,CUSTOMER_EFFECTIVE_BEGIN_TIMESTAMP
            ,CUSTOMER_EFFECTIVE_END_TIMESTAMP
        from {{ ref(customer_snp_name) }} as customer_snp
        left join {{ ref('seed__organizational_area') }} AS ref_org
                on customer_snp.CUSTOMER_ORGANIZATIONAL_AREA_ID = ref_org.ORGANIZATIONAL_AREA_ID
        left join {{ ref('seed__business_unit') }} AS ref_bu
            on ref_bu.BUSINESS_UNIT_ID = ref_org.BUSINESS_UNIT_ID
    ),
    CONTRACT_OFFERS AS (
        select
            CONTRACT_ID as contract_offers_contract_id
            ,OFFER_ID as CONTRACT_OFFER_ID
        from {{ source(source_name, contract_offers_table_name) }}
    )
    SELECT
        {{ dbt_utils.star(
            from=source(source_name, source_table_name),
            except=except_cols
        ) }},
        CUSTOMER.customer_version_id,
        CONTRACT_OFFERS.CONTRACT_OFFER_ID,
        {{ flg_portfolio_rule }} AS CONTRACT_PORTFOLIO_FLAG
    FROM {{ source(source_name, source_table_name) }} AS contract_int
    left join CUSTOMER on customer.customer_snp_customer_id = contract_int.customer_id
        and current_date between CUSTOMER.customer_effective_begin_timestamp::date and CUSTOMER.customer_effective_end_timestamp::date
        and CUSTOMER.customer_effective_end_timestamp::date = '8899-12-31'
    left join CONTRACT_OFFERS on CONTRACT_OFFERS.contract_offers_contract_id = contract_int.contract_id

{% endmacro %}