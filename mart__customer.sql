{{ config(
    enabled = (
        var('country', 'all') in ['all', 'france', 'bios', 'poland', 'mauritius']
        or (var('country', 'all') in ['m7', 'gva', 'mcg'] and target.name != 'rec')
    ),

    cluster_by               = ['customer_id','customer_effective_begin_timestamp','customer_effective_end_timestamp'],
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
        'france':    source('snapshots', 'snapshot__customer__france'),
        'bios':      source('snapshots', 'snapshot__customer__bios'),
        'poland':    source('snapshots', 'snapshot__customer__poland'),
        'mauritius': source('snapshots', 'snapshot__customer__mauritius'),
        'm7':        source('snapshots', 'snapshot__customer__m7'),
        'gva':       source('snapshots', 'snapshot__customer__gva'),
        'mcg':       source('snapshots', 'snapshot__customer__mcg')
    } %}

    {% set src = territory_source_map[territory] %}
{{ select_with_overrides(
    src,
    exclude_cols=["dbt_updated_at"],
    column_override={},
    where="customer_id IS NOT NULL"
) }}

{% else %}

    {{ dbt_utils.union_relations(
        relations=[
            source('snapshots', 'snapshot__customer__france'),
            source('snapshots', 'snapshot__customer__bios'),
            source('snapshots', 'snapshot__customer__poland'),
            source('snapshots', 'snapshot__customer__mauritius'),
        ] + ([] if target.name == 'rec' else [
            source('snapshots', 'snapshot__customer__m7'),
            source('snapshots', 'snapshot__customer__gva'),
            source('snapshots', 'snapshot__customer__mcg')
        ]),
        exclude=["dbt_updated_at"],
        where="customer_id is not null",
        source_column_name=None
    ) }}

{% endif %}

