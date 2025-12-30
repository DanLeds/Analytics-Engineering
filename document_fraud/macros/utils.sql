{% macro calculate_percentage(numerator, denominator, precision=2) %}
    round(100.0 * {{ numerator }} / greatest({{ denominator }}, 1), {{ precision }})
{% endmacro %}

{% macro calculate_status_counts(source_alias, document_id_col='document_id', status_col='control_status') %}
    count(distinct {{ source_alias }}.{{ document_id_col }}) as count_document,
    count(distinct case when {{ source_alias }}.{{ status_col }} = 'OK' then {{ source_alias }}.{{ document_id_col }} end) as count_ok,
    count(distinct case when {{ source_alias }}.{{ status_col }} = 'KO' then {{ source_alias }}.{{ document_id_col }} end) as count_ko,
    count(distinct case when {{ source_alias }}.{{ status_col }} = 'NA' then {{ source_alias }}.{{ document_id_col }} end) as count_na,
    count(distinct case when {{ source_alias }}.{{ status_col }} is null then {{ source_alias }}.{{ document_id_col }} end) as count_null
{% endmacro %}

{% macro calculate_status_percentages() %}
    {{ calculate_percentage('count_ok', 'count_document') }} as perc_ok,
    {{ calculate_percentage('count_ko', 'count_document') }} as perc_ko,
    {{ calculate_percentage('count_na', 'count_document') }} as perc_na,
    {{ calculate_percentage('count_null', 'count_document') }} as perc_null
{% endmacro %}

{% macro control_aggregation(control_name, ctl_prefix, join_columns=['issue_date', 'channel_acquisition_id', 'document_category']) %}
select
    {% for col in join_columns %}
    b.{{ col }},
    {% endfor %}
    count(distinct document_id) as {{ ctl_prefix }}_count_document,
    count(distinct case when control_status = 'OK' then document_id end) as {{ ctl_prefix }}_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) as {{ ctl_prefix }}_count_ko,
    count(distinct case when control_status = 'NA' then document_id end) as {{ ctl_prefix }}_count_na,
    count(distinct case when control_status is null then document_id end) as {{ ctl_prefix }}_count_null
from date_and_client dc
left join base b
    using ({% for col in join_columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %})
where control_name = '{{ control_name }}'
group by {% for i in range(join_columns | length) %}{{ i + 1 }}{% if not loop.last %}, {% endif %}{% endfor %}
{% endmacro %}

{% macro lookback_filter(date_column, days_var='lookback_days', default_days=730) %}
    {{ date_column }}::date >= current_date - interval '{{ var(days_var, default_days) }} days'
{% endmacro %}
