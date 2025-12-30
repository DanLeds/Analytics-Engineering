{{ config(
    materialized='table',
    tags=["nightly_1"],
    labels={"table_type": "fact_table"},
    indexes=[
        {'columns': ['control_score'], 'type': 'btree'},
        {'columns': ['control_status'], 'type': 'btree'},
        {'columns': ['created_at'], 'type': 'brin'},
        {'columns': ['control_score', 'control_status'], 'type': 'btree'},
        {'columns': ['snapshot_date'], 'type': 'brin'}
    ],
    post_hook=[
        "ANALYZE {{ this }}",
        "ALTER TABLE {{ this }} SET (autovacuum_vacuum_scale_factor = 0.1)",
        "ALTER TABLE {{ this }} SET (fillfactor = 90)"
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
    where co.issue_at::date >= current_date - interval '{{ var("lookback_days", 730) }} days'
    order by co.document_id, co.control_score, co.issue_at desc
),

aggregated_by_status as (
    select
        control_score,
        control_status,
        count(distinct document_id) as document_count,
        max(issue_at)::date as last_issue_date,
        min(issue_at)::date as first_issue_date,
        count(distinct document_category) as unique_categories,
        count(distinct document_issuing_country) as unique_countries,
        count(distinct control_name) as unique_control_names,
        count(distinct channel_acquisition_id) as unique_channels
    from base
    group by control_score, control_status
),

totals_by_score as (
    select
        control_score,
        sum(document_count) as total_documents,
        max(last_issue_date) as score_last_issue_date,
        min(first_issue_date) as score_first_issue_date
    from aggregated_by_status
    group by control_score
),

final_result as (
    select
        a.control_score,
        a.control_status,
        a.document_count,
        t.total_documents,
        {{ calculate_percentage('a.document_count', 't.total_documents') }} as percentage,
        a.first_issue_date,
        a.last_issue_date,
        a.last_issue_date - a.first_issue_date as days_span,
        a.unique_categories,
        a.unique_countries,
        a.unique_control_names,
        a.unique_channels,
        now() at time zone 'UTC' as created_at,
        current_date as snapshot_date,
        md5(
            a.control_score || '|' ||
            coalesce(a.control_status, 'NULL') || '|' ||
            current_date::text
        ) as unique_key,
        jsonb_build_object(
            'metrics', jsonb_build_object(
                'document_count', a.document_count,
                'percentage', {{ calculate_percentage('a.document_count', 't.total_documents') }},
                'total_for_score', t.total_documents
            ),
            'dates', jsonb_build_object(
                'first_date', a.first_issue_date,
                'last_date', a.last_issue_date,
                'days_span', a.last_issue_date - a.first_issue_date,
                'score_date_range', jsonb_build_object(
                    'first', t.score_first_issue_date,
                    'last', t.score_last_issue_date
                )
            ),
            'dimensions', jsonb_build_object(
                'categories', a.unique_categories,
                'countries', a.unique_countries,
                'control_names', a.unique_control_names,
                'channels', a.unique_channels
            )
        ) as metrics_json
    from aggregated_by_status a
    inner join totals_by_score t
        on a.control_score = t.control_score
)

select
    control_score,
    control_status,
    document_count,
    total_documents,
    percentage,
    first_issue_date,
    last_issue_date,
    days_span,
    unique_categories,
    unique_countries,
    unique_control_names,
    unique_channels,
    metrics_json,
    created_at,
    snapshot_date,
    unique_key
from final_result
order by
    control_score,
    case
        when control_status = 'OK' then 1
        when control_status = 'KO' then 2
        when control_status = 'NA' then 3
        when control_status is null then 4
        else 5
    end
