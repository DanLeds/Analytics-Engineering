{{ config(
    materialized='table',
    tags=["nightly_1"], 
    labels={
      "table_type": "aggregate_table"
    }
) }}


with DATE_AND_CLIENT as (
select 
    issue_date,
    channel_acquisition_id, 
    document_category
from generate_series('2025-01-01', current_date, interval '1 month') as issue_date,
     unnest(array['client 1', 'client 2', 'client na']) as channel_acquisition_id,
     unnest(array['address_proof', 'bank_account_details', 'digitalid',
                  'driving_license', 'id_card' , 'open_banking',
                  'passport', 'payslip', 'residence_permit',
                  'tax_document', 'bank_account_details', 'digitalid'
             ]) as document_category
)
,BASE as (
select 
    distinct co.document_id,
    co.event_id,
    co.issue_at::timestamp,
    date_trunc('month',date(co.issue_at)) as issue_date, 
    co.control_status,
    co.control_name,
    coalesce(co.channel_acquisition_id, 'client na') as channel_acquisition_id,
    cr.document_category,
    cr.document_issuing_country,
    cr.document_issue_year
--from analytics."V_STG_DOCUMENTS_CONTROLLED" co
from {{ref('V_STG_DOCUMENTS_CONTROLLED')}} co
--inner join analytics."V_STG_DOCUMENTS_CREATED" cr
left join {{ref('V_STG_DOCUMENTS_CREATED')}} cr
using(document_id)
where 1 = 1 
    and co.issue_at::date >= current_date - 730 
)
,CTL01_ADDRESS_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl01_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl01_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl01_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl01_count_na, 
    count(distinct case when control_status is null then document_id end) ctl01_count_null
from DATE_AND_CLIENT dc
left join BASE b 
using(issue_date, channel_acquisition_id, document_category)
where 1 = 1
    and control_name = 'address_consistency'
group by 1, 2, 3
)
,CTL02_ADDRESS_LOCALITY_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl02_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl02_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl02_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl02_count_na, 
    count(distinct case when control_status is null then document_id end) ctl02_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(issue_date, channel_acquisition_id, document_category)
where 1 = 1
    and control_name = 'address_locality_consistency'
group by 1, 2, 3
)
,CTL03_ALTERNATE_FAMILY_NAME_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl03_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl03_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl03_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl03_count_na, 
    count(distinct case when control_status is null then document_id end) ctl03_count_null
from DATE_AND_CLIENT dc 
left join BASE b
using(issue_date, channel_acquisition_id, document_category)
where 1 = 1
    and control_name = 'alternate_family_name_consistency'
group by 1, 2, 3
)
,CTL04_ALTERNATE_FAMILY_NAME_DETECTION as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl04_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl04_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl04_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl04_count_na, 
    count(distinct case when control_status is null then document_id end) ctl04_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(issue_date, channel_acquisition_id, document_category)
where 1 = 1
    and control_name = 'alternate_family_name_detection'
group by 1, 2, 3
)
,CTL05_BIC_IBAN_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl05_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl05_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl05_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl05_count_na, 
    count(distinct case when control_status is null then document_id end) ctl05_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(issue_date, channel_acquisition_id, document_category)
where 1 = 1
    and control_name = 'bic_iban_consistency'
group by 1, 2, 3
)
,CTL06_BIRTHDATE_MRZ_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl06_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl06_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl06_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl06_count_na, 
    count(distinct case when control_status is null then document_id end) ctl06_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(issue_date, channel_acquisition_id, document_category)
where 1 = 1
    and control_name = 'birthdate_mrz_consistency'
group by 1, 2, 3
)
,CTL07_DATE_VALIDATION as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl07_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl07_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl07_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl07_count_na, 
    count(distinct case when control_status is null then document_id end) ctl07_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(channel_acquisition_id)
where 1 = 1
    and control_name = 'date_validation'
group by 1, 2, 3
)
,CTL08_FAMILY_NAME_ACCOUNT_HOLDER_SEARCH as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl08_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl08_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl08_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl08_count_na, 
    count(distinct case when control_status is null then document_id end) ctl08_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(channel_acquisition_id)
where 1 = 1
    and control_name = 'family_name_account_holder_search'
group by 1, 2, 3
)
,CTL09_FAMILY_NAME_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl09_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl09_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl09_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl09_count_na, 
    count(distinct case when control_status is null then document_id end) ctl09_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(channel_acquisition_id)
where 1 = 1
    and control_name = 'family_name_consistency'
group by 1, 2, 3
)
,CTL10_FAMILY_NAME_SEARCH as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl10_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl10_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl10_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl10_count_na, 
    count(distinct case when control_status is null then document_id end) ctl10_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(channel_acquisition_id)
where 1 = 1
    and control_name = 'family_name_search'
group by 1, 2, 3
)
,CTL11_PERSONAL_ID_CONSISTENCY as (
select 
    b.issue_date,
    b.channel_acquisition_id,
    b.document_category,
    count(distinct document_id) ctl11_count_document,
    count(distinct case when control_status = 'OK' then document_id end) ctl11_count_ok,
    count(distinct case when control_status = 'KO' then document_id end) ctl11_count_ko, 
    count(distinct case when control_status = 'NA' then document_id end) ctl11_count_na, 
    count(distinct case when control_status is null then document_id end) ctl11_count_null
from DATE_AND_CLIENT dc
left join BASE b
using(channel_acquisition_id)
where 1 = 1
    and control_name = 'personal_id_consistency'
group by 1, 2, 3
)
,final as (
select 
    dc.issue_date,
    dc.channel_acquisition_id,
    dc.document_category,
         coalesce(ctl01_count_document, 0) as   ctl01_count_document,
            coalesce(ctl01_count_ok, 0) as    ctl01_count_ok,
            coalesce(ctl01_count_ko, 0) as    ctl01_count_ko, 
            coalesce(ctl01_count_na, 0) as    ctl01_count_na, 
            coalesce(ctl01_count_null, 0) as    ctl01_count_null,
            coalesce(ctl02_count_document, 0) as    ctl02_count_document,
            coalesce(ctl02_count_ok, 0) as    ctl02_count_ok,
            coalesce(ctl02_count_ko, 0) as    ctl02_count_ko, 
            coalesce(ctl02_count_na, 0) as    ctl02_count_na, 
            coalesce(ctl02_count_null, 0) as    ctl02_count_null,
            coalesce(ctl03_count_document, 0) as    ctl03_count_document,
            coalesce(ctl03_count_ok, 0) as    ctl03_count_ok,
            coalesce(ctl03_count_ko, 0) as    ctl03_count_ko, 
            coalesce(ctl03_count_na, 0) as    ctl03_count_na, 
            coalesce(ctl03_count_null, 0) as    ctl03_count_null,
            coalesce(ctl04_count_document, 0) as    ctl04_count_document,
            coalesce(ctl04_count_ok, 0) as    ctl04_count_ok,
            coalesce(ctl04_count_ko, 0) as    ctl04_count_ko, 
            coalesce(ctl04_count_na, 0) as    ctl04_count_na, 
            coalesce(ctl04_count_null, 0)      as    ctl04_count_null,
            coalesce(ctl05_count_document, 0) as    ctl05_count_document,
            coalesce(ctl05_count_ok, 0) as    ctl05_count_ok,
            coalesce(ctl05_count_ko, 0) as    ctl05_count_ko, 
            coalesce(ctl05_count_na, 0) as    ctl05_count_na, 
            coalesce(ctl05_count_null, 0) as    ctl05_count_null,
            coalesce(ctl06_count_document, 0) as    ctl06_count_document,
            coalesce(ctl06_count_ok, 0) as    ctl06_count_ok,
            coalesce(ctl06_count_ko, 0) as    ctl06_count_ko, 
            coalesce(ctl06_count_na, 0) as    ctl06_count_na, 
            coalesce(ctl06_count_null, 0) as    ctl06_count_null,
            coalesce(ctl07_count_document, 0) as    ctl07_count_document,
            coalesce(ctl07_count_ok, 0) as    ctl07_count_ok,
            coalesce(ctl07_count_ko, 0) as    ctl07_count_ko, 
            coalesce(ctl07_count_na, 0) as    ctl07_count_na, 
            coalesce(ctl07_count_null, 0) as    ctl07_count_null,
            coalesce(ctl08_count_document, 0) as    ctl08_count_document,
            coalesce(ctl08_count_ok, 0) as    ctl08_count_ok,
            coalesce(ctl08_count_ko, 0) as    ctl08_count_ko, 
            coalesce(ctl08_count_na, 0) as    ctl08_count_na, 
            coalesce(ctl08_count_null, 0) as    ctl08_count_null,
            coalesce(ctl09_count_document, 0) as    ctl09_count_document,
            coalesce(ctl09_count_ok, 0) as    ctl09_count_ok,
            coalesce(ctl09_count_ko, 0) as    ctl09_count_ko, 
            coalesce(ctl09_count_na, 0) as    ctl09_count_na, 
            coalesce(ctl09_count_null, 0) as    ctl09_count_null,
            coalesce(ctl10_count_document, 0) as    ctl10_count_document,
            coalesce(ctl10_count_ok, 0) as    ctl10_count_ok,
            coalesce(ctl10_count_ko, 0) as    ctl10_count_ko, 
            coalesce(ctl10_count_na, 0) as    ctl10_count_na, 
            coalesce(ctl10_count_null, 0) as    ctl10_count_null,
            coalesce(ctl11_count_document, 0) as    ctl11_count_document,
            coalesce(ctl11_count_ok, 0) as    ctl11_count_ok,
            coalesce(ctl11_count_ko, 0) as    ctl11_count_ko, 
            coalesce(ctl11_count_na, 0) as    ctl11_count_na, 
            coalesce(ctl11_count_null, 0)    as    ctl11_count_null,
    concat( dc.issue_date,
            dc.channel_acquisition_id,
            dc.document_category,
            coalesce(ctl01_count_document, 0),
            coalesce(ctl01_count_ok, 0),
            coalesce(ctl01_count_ko, 0), 
            coalesce(ctl01_count_na, 0),
            coalesce(ctl01_count_null, 0),
            coalesce(ctl02_count_document, 0),
            coalesce(ctl02_count_ok, 0),
            coalesce(ctl02_count_ko, 0), 
            coalesce(ctl02_count_na, 0),
            coalesce(ctl02_count_null, 0),
            coalesce(ctl03_count_document, 0),
            coalesce(ctl03_count_ok, 0),
            coalesce(ctl03_count_ko, 0), 
            coalesce(ctl03_count_na, 0), 
            coalesce(ctl03_count_null, 0),
            coalesce(ctl04_count_document, 0),
            coalesce(ctl04_count_ok, 0),
            coalesce(ctl04_count_ko, 0), 
            coalesce(ctl04_count_na, 0), 
            coalesce(ctl04_count_null, 0),            
            coalesce(ctl05_count_document, 0),
            coalesce(ctl05_count_ok, 0),
            coalesce(ctl05_count_ko, 0), 
            coalesce(ctl05_count_na, 0),
            coalesce(ctl05_count_null, 0),
            coalesce(ctl06_count_document, 0),
            coalesce(ctl06_count_ok, 0),
            coalesce(ctl06_count_ko, 0), 
            coalesce(ctl06_count_na, 0),
            coalesce(ctl06_count_null, 0),
            coalesce(ctl07_count_document, 0),
            coalesce(ctl07_count_ok, 0),
            coalesce(ctl07_count_ko, 0), 
            coalesce(ctl07_count_na, 0), 
            coalesce(ctl07_count_null, 0),
            coalesce(ctl08_count_document, 0),
            coalesce(ctl08_count_ok, 0),
            coalesce(ctl08_count_ko, 0), 
            coalesce(ctl08_count_na, 0), 
            coalesce(ctl08_count_null, 0),     
            coalesce(ctl09_count_document, 0),
            coalesce(ctl09_count_ok, 0),
            coalesce(ctl09_count_ko, 0), 
            coalesce(ctl09_count_na, 0), 
            coalesce(ctl09_count_null, 0),    
            coalesce(ctl10_count_document, 0),
            coalesce(ctl10_count_ok, 0),
            coalesce(ctl10_count_ko, 0), 
            coalesce(ctl10_count_na, 0), 
            coalesce(ctl10_count_null, 0),    
            coalesce(ctl11_count_document, 0),
            coalesce(ctl11_count_ok, 0),
            coalesce(ctl11_count_ko, 0), 
            coalesce(ctl11_count_na, 0), 
            coalesce(ctl11_count_null, 0)    
            ) as unique_key
from DATE_AND_CLIENT dc
left join CTL01_ADDRESS_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)
left join CTL02_ADDRESS_LOCALITY_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)
left join CTL03_ALTERNATE_FAMILY_NAME_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)
left join CTL04_ALTERNATE_FAMILY_NAME_DETECTION
using(issue_date, channel_acquisition_id, document_category)
left join CTL05_BIC_IBAN_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)  
left join CTL06_BIRTHDATE_MRZ_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)  
left join CTL07_DATE_VALIDATION
using(issue_date, channel_acquisition_id, document_category)  
left join CTL08_FAMILY_NAME_ACCOUNT_HOLDER_SEARCH
using(issue_date, channel_acquisition_id, document_category)
left join CTL09_FAMILY_NAME_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)  
left join CTL10_FAMILY_NAME_SEARCH
using(issue_date, channel_acquisition_id, document_category)  
left join CTL11_PERSONAL_ID_CONSISTENCY
using(issue_date, channel_acquisition_id, document_category)  
where 1 = 1
)
,final2 as (
select *, 
    row_number() over(partition by unique_key order by issue_date desc) unique_row
from final 
)
select * 
from final2 
where 1 = 1 
and unique_row = 1 