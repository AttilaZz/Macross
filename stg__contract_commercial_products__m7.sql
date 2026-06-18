select
    src.CRM_ID
    ,src.CONTRACT_ID
    ,src.COMMERCIAL_PRODUCT_ID
    ,CONTRACT_COMMERCIAL_PRODUCT_START_DATE as CONTRACT_COMMERCIAL_PRODUCT_START_TIMESTAMP
    ,CONTRACT_COMMERCIAL_PRODUCT_END_DATE as CONTRACT_COMMERCIAL_PRODUCT_END_TIMESTAMP
    ,snp.CUSTOMER_COUNTRY_ID
    ,snp.CONTRACT_VERSION_ID
    ,src.UPDATE_TIMESTAMP
from
{{ source('customer_m7','interface__contract_commercial_products__m7') }} as  src
left join {{ ref('snapshot__contract__m7') }} as snp on src.contract_id = snp.contract_id
    and current_date between snp.CONTRACT_EFFECTIVE_BEGIN_TIMESTAMP::date and snp.CONTRACT_EFFECTIVE_END_TIMESTAMP::date
    and snp.CONTRACT_EFFECTIVE_END_TIMESTAMP::date = '8899-12-31'
