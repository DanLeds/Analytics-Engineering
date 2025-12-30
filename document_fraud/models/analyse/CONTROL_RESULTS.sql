{{ config(
    materialized='table',
    tags=["nightly_1"],
    labels={"table_type": "aggregate_table"},
    indexes=[
        {'columns': ['control_client_document']},
        {'columns': ['count_document']}
    ]
) }}

with base as (
    select distinct
        co.document_id,
        co.control_status,
        co.control_name,
        co.channel_acquisition_id,
        cr.document_category,
        cr.document_issuing_country,
        cr.document_issue_year,
        co.issue_at
    from {{ ref('V_STG_DOCUMENTS_CONTROLLED') }} co
    inner join {{ ref('V_STG_DOCUMENTS_CREATED') }} cr
        using (document_id)
    where {{ lookback_filter('co.issue_at') }}
),

aggregated as (
    select
        dimension_name,
        dimension_value,
        count(distinct document_id) as count_document,
        count(distinct case when control_status = 'OK' then document_id end) as count_ok,
        count(distinct case when control_status = 'KO' then document_id end) as count_ko,
        count(distinct case when control_status = 'NA' then document_id end) as count_na,
        count(distinct case when control_status is null then document_id end) as count_null
    from (
        select document_id, control_status, 'control_name' as dimension_name, control_name as dimension_value from base
        union all
        select document_id, control_status, 'channel_acquisition_id', channel_acquisition_id from base
        union all
        select document_id, control_status, 'document_category', document_category from base
        union all
        select document_id, control_status, 'document_issuing_country', document_issuing_country from base
        union all
        select document_id, control_status, 'document_issue_year', document_issue_year from base
    ) unpivoted
    group by dimension_name, dimension_value
)

select
    dimension_value as control_client_document,
    count_document,
    count_ok,
    {{ calculate_percentage('count_ok', 'count_document') }} as perc_ok,
    count_ko,
    {{ calculate_percentage('count_ko', 'count_document') }} as perc_ko,
    count_na,
    {{ calculate_percentage('count_na', 'count_document') }} as perc_na,
    count_null,
    {{ calculate_percentage('count_null', 'count_document') }} as perc_null,
    md5(
        coalesce(dimension_name, '') || '|' ||
        coalesce(dimension_value, '') || '|' ||
        current_date::text
    ) as unique_key
from aggregated
