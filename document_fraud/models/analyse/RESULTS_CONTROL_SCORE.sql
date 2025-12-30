{{ config(
    materialized='table',
    tags=["nightly_1"],
    labels={"table_type": "fact_table"},
    indexes=[
        {'columns': ['control_score'], 'type': 'btree'},
        {'columns': ['last_issue_date'], 'type': 'brin'},
        {'columns': ['snapshot_date'], 'type': 'brin'}
    ],
    post_hook=[
        "ANALYZE {{ this }}",
        "ALTER TABLE {{ this }} SET (autovacuum_vacuum_scale_factor = 0.1)",
        "ALTER TABLE {{ this }} SET (fillfactor = 90)",
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_metrics_json ON {{ this }} USING GIN (metrics_json)"
    ]
) }}

with base as (
    select
        distinct on (co.document_id, co.control_score)
        co.document_id,
        co.control_status,
        co.issue_at::timestamptz,
        coalesce(co.control_score, 'controlna') as control_score,
        co.control_name,
        co.channel_acquisition_id,
        cr.document_category,
        cr.document_issuing_country,
        cr.document_issue_year
    from {{ ref('V_STG_DOCUMENTS_CONTROLLED') }} co
    left join {{ ref('V_STG_DOCUMENTS_CREATED') }} cr
        using (document_id)
    where {{ lookback_filter('co.issue_at') }}
    order by co.document_id, co.control_score, co.issue_at desc
),

aggregated_metrics as (
    select
        control_score,
        count(distinct document_id) as total_documents,
        count(distinct document_id) filter (where control_status = 'OK') as count_ok,
        count(distinct document_id) filter (where control_status = 'KO') as count_ko,
        count(distinct document_id) filter (where control_status = 'NA') as count_na,
        count(distinct document_id) filter (where control_status is null) as count_null,
        max(issue_at)::date as last_issue_date,
        min(issue_at)::date as first_issue_date,
        count(distinct document_category) as unique_categories,
        count(distinct document_issuing_country) as unique_countries
    from base
    group by control_score
),

final_result as (
    select
        control_score,
        total_documents,
        count_ok,
        count_ko,
        count_na,
        count_null,
        {{ calculate_percentage('count_ok', 'total_documents') }} as perc_ok,
        {{ calculate_percentage('count_ko', 'total_documents') }} as perc_ko,
        {{ calculate_percentage('count_na', 'total_documents') }} as perc_na,
        {{ calculate_percentage('count_null', 'total_documents') }} as perc_null,
        first_issue_date,
        last_issue_date,
        last_issue_date - first_issue_date as days_span,
        unique_categories,
        unique_countries,
        now() at time zone 'UTC' as created_at,
        current_date as snapshot_date,
        md5(control_score || '|' || current_date::text) as unique_key,
        jsonb_build_object(
            'counts', jsonb_build_object(
                'ok', count_ok,
                'ko', count_ko,
                'na', count_na,
                'null', count_null,
                'total', total_documents
            ),
            'percentages', jsonb_build_object(
                'ok_pct', {{ calculate_percentage('count_ok', 'total_documents') }},
                'ko_pct', {{ calculate_percentage('count_ko', 'total_documents') }},
                'na_pct', {{ calculate_percentage('count_na', 'total_documents') }},
                'null_pct', {{ calculate_percentage('count_null', 'total_documents') }}
            ),
            'metadata', jsonb_build_object(
                'first_date', first_issue_date,
                'last_date', last_issue_date,
                'days_span', last_issue_date - first_issue_date,
                'unique_categories', unique_categories,
                'unique_countries', unique_countries
            )
        ) as metrics_json
    from aggregated_metrics
)

select
    control_score,
    total_documents,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,
    count_na,
    perc_na,
    count_null,
    perc_null,
    first_issue_date,
    last_issue_date,
    days_span,
    unique_categories,
    unique_countries,
    metrics_json,
    created_at,
    snapshot_date,
    unique_key
from final_result
order by control_score
