{{ config(
    materialized='view',
    tags=["nightly_1"],
    labels={"table_type": "aggregate_table"}
) }}

with monthly_data as (
    select
        date_trunc('month', co.issue_at::timestamptz) as issue_month,
        coalesce(co.control_score, 'controlna') as control_score,
        co.control_status,
        count(distinct co.document_id) as document_count
    from {{ ref('V_STG_DOCUMENTS_CONTROLLED') }} co
    where co.issue_at::timestamptz >= current_date - interval '12 months'
    group by 1, 2, 3
),

monthly_summary as (
    select
        issue_month,
        control_score,
        sum(document_count) as total_count,
        sum(document_count) filter (where control_status = 'OK') as count_ok,
        sum(document_count) filter (where control_status = 'KO') as count_ko,
        sum(document_count) filter (where control_status = 'NA') as count_na,
        sum(document_count) filter (where control_status is null) as count_null
    from monthly_data
    group by issue_month, control_score
)

select
    issue_month::date as issue_month_date,
    control_score,
    total_count,
    count_ok,
    count_ko,
    count_na,
    count_null,
    {{ calculate_percentage('count_ok', 'total_count') }} as perc_ok,
    {{ calculate_percentage('count_ko', 'total_count') }} as perc_ko,
    lag(total_count, 1) over (partition by control_score order by issue_month) as prev_month_count,
    total_count - lag(total_count, 1) over (partition by control_score order by issue_month) as mom_change,
    {{ calculate_percentage(
        'total_count - lag(total_count, 1) over (partition by control_score order by issue_month)',
        'lag(total_count, 1) over (partition by control_score order by issue_month)'
    ) }} as mom_growth_pct
from monthly_summary
order by control_score, issue_month desc
