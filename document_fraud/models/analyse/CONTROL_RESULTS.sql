{{ config(
    materialized='table',
    tags=["nightly_1"], 
    labels={
      "table_type": "aggregate_table"
    }
) }}

with BASE as (
select 
    distinct co.document_id,
    co.control_status,
    co.control_name,
    co.channel_acquisition_id,
    cr.document_category,
    cr.document_issuing_country,
    cr.document_issue_year,
    co.issue_at
--from analytics."V_STG_DOCUMENTS_CONTROLLED" co
from {{ref('V_STG_DOCUMENTS_CONTROLLED')}} co
--inner join analytics."V_STG_DOCUMENTS_CREATED" cr
inner join {{ref('V_STG_DOCUMENTS_CREATED')}} cr
using(document_id) 
where 1 = 1 
    and co.issue_at::date >= current_date - 730 
)
,RESULT_CONTROL_NAME as (
select 
    control_name,
    count(distinct document_id) as count_document,
    count(distinct case when control_status = 'OK' then document_id end) as count_ok,
    cast(count(distinct case when control_status = 'OK' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ok,
    count(distinct case when control_status = 'KO' then document_id end) as count_ko,
    cast(count(distinct case when control_status = 'KO' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ko,    
    count(distinct case when control_status = 'NA' then document_id end) as count_na,
    cast(count(distinct case when control_status = 'NA' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_na,    
    count(distinct case when control_status is null then document_id end) as count_null,
    cast(count(distinct case when control_status is null then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_null
from BASE 
where 1 = 1
group by 1
)
,RESULT_CLIENT_ACQUISITION as (
select 
    channel_acquisition_id,
    count(distinct document_id) as count_document,
    count(distinct case when control_status = 'OK' then document_id end) as count_ok,
    cast(count(distinct case when control_status = 'OK' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ok,
    count(distinct case when control_status = 'KO' then document_id end) as count_ko,
    cast(count(distinct case when control_status = 'KO' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ko,    
    count(distinct case when control_status = 'NA' then document_id end) as count_na,
    cast(count(distinct case when control_status = 'NA' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_na,    
    count(distinct case when control_status is null then document_id end) as count_null,
    cast(count(distinct case when control_status is null then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_null
from BASE 
where 1 = 1
group by 1
)
,RESULT_DOCUMENT_CATEGORY as (
select 
    document_category,
    count(distinct document_id) as count_document,
    count(distinct case when control_status = 'OK' then document_id end) as count_ok,
    cast(count(distinct case when control_status = 'OK' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ok,
    count(distinct case when control_status = 'KO' then document_id end) as count_ko,
    cast(count(distinct case when control_status = 'KO' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ko,    
    count(distinct case when control_status = 'NA' then document_id end) as count_na,
    cast(count(distinct case when control_status = 'NA' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_na,    
    count(distinct case when control_status is null then document_id end) as count_null,
    cast(count(distinct case when control_status is null then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_null
from BASE 
where 1 = 1
group by 1
)
,RESULT_DOCUMENT_COUNTRY as (
select 
    document_issuing_country,
    count(distinct document_id) as count_document,
    count(distinct case when control_status = 'OK' then document_id end) as count_ok,
    cast(count(distinct case when control_status = 'OK' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ok,
    count(distinct case when control_status = 'KO' then document_id end) as count_ko,
    cast(count(distinct case when control_status = 'KO' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ko,    
    count(distinct case when control_status = 'NA' then document_id end) as count_na,
    cast(count(distinct case when control_status = 'NA' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_na,    
    count(distinct case when control_status is null then document_id end) as count_null,
    cast(count(distinct case when control_status is null then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_null
from BASE 
where 1 = 1
group by 1
)
,RESULT_DOCUMENT_YEAR as (
select 
    document_issue_year,
    count(distinct document_id) as count_document,
    count(distinct case when control_status = 'OK' then document_id end) as count_ok,
    cast(count(distinct case when control_status = 'OK' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ok,
    count(distinct case when control_status = 'KO' then document_id end) as count_ko,
    cast(count(distinct case when control_status = 'KO' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_ko,    
    count(distinct case when control_status = 'NA' then document_id end) as count_na,
    cast(count(distinct case when control_status = 'NA' then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_na,    
    count(distinct case when control_status is null then document_id end) as count_null,
    cast(count(distinct case when control_status is null then document_id end) as float) / cast(count(distinct document_id) as float) *100 as perc_null
from BASE 
where 1 = 1
group by 1
)
,final as (
select 
    control_name as control_client_document,
    count_document,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,    
    count_na,
    perc_na,    
    count_null,
    perc_null 
from RESULT_CONTROL_NAME
union all
select
    channel_acquisition_id as control_client_document,
    count_document,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,    
    count_na,
    perc_na,    
    count_null,
    perc_null 
from RESULT_CLIENT_ACQUISITION
union all
select
    document_category as control_client_document,
    count_document,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,    
    count_na,
    perc_na,    
    count_null,
    perc_null 
from RESULT_DOCUMENT_CATEGORY
union all
select 
    document_issuing_country as control_client_document,
    count_document,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,    
    count_na,
    perc_na,    
    count_null,
    perc_null 
from RESULT_DOCUMENT_COUNTRY
union all
select 
    document_issue_year as control_client_document,
    count_document,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,    
    count_na,
    perc_na,    
    count_null,
    perc_null 
from RESULT_DOCUMENT_YEAR
)
select 
    control_client_document,
    count_document,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,    
    count_na,
    perc_na,    
    count_null,
    perc_null ,
    concat(coalesce(control_client_document, '0'),
           coalesce(count_document, '0'),
           coalesce(count_ok, '0'),
           coalesce(perc_ok, '0'),
           coalesce(count_ko, '0'),
           coalesce(perc_ko, '0'),
           coalesce(count_na, '0'),
           coalesce(perc_na, '0'),    
           coalesce(count_null, '0'),
           coalesce(perc_null,  '0')
    ) as unique_key
from final
where 1 = 1