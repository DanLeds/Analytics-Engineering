{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='unique_key',
    on_schema_change='append_new_columns',
    tags=["nightly_1"] 
) }}

with fusion as (
select 
    cr.event_id as event_creation_document_id,
    cr.document_id,
    cr.receipt_at as document_receipt_at,
    cr.channel_acquisition_id as document_channel_acquisition_id,
    cr.acquisition_type, 
    cr.document_subtype, 
    cr.document_category, 
    cr.document_issuing_country, 
    cr.document_issue_year, 
    co.event_id as event_control_id,
    co.control_applied_status,
    co.receipt_at as control_receipt_at,
    co.control_score ,
    co.issue_at as control_controled_issue_at,
    co.control_status,
    co.document_face_id,
    co.document_type,
    co.control_domain,
    co.channel_acquisition_id as control_channel_acquisition_id,
    co.control_subject,
    co.control_name
--from analytics."V_STG_DOCUMENTS_CONTROLLED" co
from {{ref('V_STG_DOCUMENTS_CONTROLLED')}} co
--inner join analytics."V_STG_DOCUMENTS_CREATED" cr
left join {{ref('V_STG_DOCUMENTS_CREATED')}} cr
using(document_id)
where 1 = 1  
{% if is_incremental() %}
    and co.issue_at > (select MAX(control_controled_issue_at) from {{ this }})
{% else %}
    and co.issue_at::date >= CURRENT_DATE - 730 
{% endif %}
)
select  
    *,
    concat(
    event_creation_document_id,
    document_id,
    document_receipt_at,
    document_channel_acquisition_id,
    acquisition_type, 
    document_subtype, 
    document_category, 
    document_issuing_country, 
    document_issue_year, 
    event_control_id,
    control_applied_status,
    control_receipt_at,
    control_score ,
    control_controled_issue_at,
    control_status,
    document_face_id,
    document_type,
    control_domain,
    control_channel_acquisition_id,
    control_subject,
    control_name
    ) as unique_key
from fusion 
where 1 = 1