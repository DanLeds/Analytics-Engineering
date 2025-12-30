{{ config(
    materialized='table',
    tags=["nightly_1"],
    labels={"table_type": "aggregate_table"},
    indexes=[
        {'columns': ['issue_date'], 'type': 'brin'},
        {'columns': ['channel_acquisition_id']},
        {'columns': ['control_name']}
    ]
) }}

{%- set control_names = [
    'address_consistency',
    'address_locality_consistency',
    'alternate_family_name_consistency',
    'alternate_family_name_detection',
    'bic_iban_consistency',
    'birthdate_mrz_consistency',
    'date_validation',
    'family_name_account_holder_search',
    'family_name_consistency',
    'family_name_search',
    'personal_id_consistency'
] -%}

{%- set channels = var('channels', ['client 1', 'client 2', 'client na']) -%}

{%- set document_categories = var('document_categories', [
    'address_proof', 'bank_account_details', 'digitalid',
    'driving_license', 'id_card', 'open_banking',
    'passport', 'payslip', 'residence_permit', 'tax_document'
]) -%}

with date_dimension as (
    select issue_date::date as issue_date
    from generate_series(
        date_trunc('month', current_date - interval '{{ var("lookback_days", 730) }} days'),
        current_date,
        interval '1 month'
    ) as issue_date
),

channels as (
    select unnest(array[{% for c in channels %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}]) as channel_acquisition_id
),

categories as (
    select unnest(array[{% for c in document_categories %}'{{ c }}'{% if not loop.last %}, {% endif %}{% endfor %}]) as document_category
),

date_and_client as (
    select
        d.issue_date,
        ch.channel_acquisition_id,
        cat.document_category
    from date_dimension d
    cross join channels ch
    cross join categories cat
),

base as (
    select distinct
        co.document_id,
        co.event_id,
        co.issue_at::timestamp,
        date_trunc('month', co.issue_at::date)::date as issue_date,
        co.control_status,
        co.control_name,
        coalesce(co.channel_acquisition_id, 'client na') as channel_acquisition_id,
        cr.document_category
    from {{ ref('V_STG_DOCUMENTS_CONTROLLED') }} co
    left join {{ ref('V_STG_DOCUMENTS_CREATED') }} cr
        using (document_id)
    where {{ lookback_filter('co.issue_at') }}
),

control_aggregations as (
    select
        b.issue_date,
        b.channel_acquisition_id,
        b.document_category,
        b.control_name,
        count(distinct b.document_id) as count_document,
        count(distinct case when b.control_status = 'OK' then b.document_id end) as count_ok,
        count(distinct case when b.control_status = 'KO' then b.document_id end) as count_ko,
        count(distinct case when b.control_status = 'NA' then b.document_id end) as count_na,
        count(distinct case when b.control_status is null then b.document_id end) as count_null
    from base b
    group by 1, 2, 3, 4
),

pivoted as (
    select
        dc.issue_date,
        dc.channel_acquisition_id,
        dc.document_category,
        {%- for ctl in control_names %}
        {%- set ctl_num = '%02d' % loop.index %}
        {%- set ctl_short = 'ctl' ~ ctl_num %}
        coalesce(max(case when ca.control_name = '{{ ctl }}' then ca.count_document end), 0) as {{ ctl_short }}_count_document,
        coalesce(max(case when ca.control_name = '{{ ctl }}' then ca.count_ok end), 0) as {{ ctl_short }}_count_ok,
        coalesce(max(case when ca.control_name = '{{ ctl }}' then ca.count_ko end), 0) as {{ ctl_short }}_count_ko,
        coalesce(max(case when ca.control_name = '{{ ctl }}' then ca.count_na end), 0) as {{ ctl_short }}_count_na,
        coalesce(max(case when ca.control_name = '{{ ctl }}' then ca.count_null end), 0) as {{ ctl_short }}_count_null{% if not loop.last %},{% endif %}
        {%- endfor %}
    from date_and_client dc
    left join control_aggregations ca
        on dc.issue_date = ca.issue_date
        and dc.channel_acquisition_id = ca.channel_acquisition_id
        and dc.document_category = ca.document_category
    group by 1, 2, 3
)

select
    issue_date,
    channel_acquisition_id,
    document_category,
    {%- for ctl in control_names %}
    {%- set ctl_num = '%02d' % loop.index %}
    {%- set ctl_short = 'ctl' ~ ctl_num %}
    {{ ctl_short }}_count_document,
    {{ ctl_short }}_count_ok,
    {{ ctl_short }}_count_ko,
    {{ ctl_short }}_count_na,
    {{ ctl_short }}_count_null{% if not loop.last %},{% endif %}
    {%- endfor %},
    md5(
        issue_date::text || '|' ||
        channel_acquisition_id || '|' ||
        document_category
    ) as unique_key
from pivoted
order by issue_date desc, channel_acquisition_id, document_category
