{{ config(
    enabled = (
        var('country', 'all') in ['all', 'france', 'bios', 'mauritius']
        or (var('country', 'all') in ['m7', 'poland', 'gva', 'mcg'] and target.name != 'rec')
    ),

    cluster_by               = ['crm_id','contract_effective_begin_timestamp','contract_effective_end_timestamp'],
    materialized             = 'incremental',
    incremental_strategy     = 'delete+insert',
    unique_key               = 'crm_id',
    on_schema_change         = 'ignore',
    persist_docs             = {"relation": false, "columns": false},

    post_hook = [
        "{{ apply_rap_on_create_or_full_refresh('ROW_ACCESS_POLICY_CRM_COUNTRY_ROLE', ['CRM_ID','CUSTOMER_COUNTRY_ID']) }}"
    ]
) }}

{% set territory = var('country', 'all') %}

{% if is_incremental() and territory != 'all' %}

    {% set territory_source_map = {
        'france':    source('snapshots', 'snapshot__contract__france'),
        'bios':      source('snapshots', 'snapshot__contract__bios'),
        'poland':    source('snapshots', 'snapshot__contract__poland'),
        'mauritius': source('snapshots', 'snapshot__contract__mauritius'),
        'm7':        source('snapshots', 'snapshot__contract__m7'),
        'gva':        source('snapshots', 'snapshot__contract__gva'),
        'mcg':        source('snapshots', 'snapshot__contract__mcg')
    } %}

    {% set src = territory_source_map[territory] %}

    {{ select_with_overrides(
        src,
        exclude_cols=["dbt_updated_at"],
        column_override={"CONTRACT_PAYMENT_METHOD_LOCAL_CODE": "character varying(16777216)"},
        where="contract_id is not null"
    ) }}

{% else %}

    {{ dbt_utils.union_relations(
        relations=[
            source('snapshots', 'snapshot__contract__france'),
            source('snapshots', 'snapshot__contract__bios'),
            source('snapshots', 'snapshot__contract__mauritius'),
        ] + ([] if target.name == 'rec' else [
            source('snapshots', 'snapshot__contract__poland'),
            source('snapshots', 'snapshot__contract__m7'),
            source('snapshots', 'snapshot__contract__gva'),
            source('snapshots', 'snapshot__contract__mcg')

        ]),
        exclude=["dbt_updated_at"],
        where="contract_id is not null",
        column_override={"CONTRACT_PAYMENT_METHOD_LOCAL_CODE": "character varying(16777216)"},
        source_column_name=None
    ) }}

{% endif %}

