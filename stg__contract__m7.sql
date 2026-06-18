
{{ contract_enrich(
    source_name='customer_m7',
    source_table_name='interface__contract__m7',
    customer_snp_name='snapshot__customer__m7',
    flg_portfolio_rule="iff(CONTRACT_FREE_FLAG = 0 and CONTRACT_TEST_PROFILE_FLAG = 0,1,0)",
    contract_offers_table_name='interface__contract_offers__m7',
    except_cols=["CONTRACT_ACTIVE_PORTFOLIO_FLAG"]
) }}
