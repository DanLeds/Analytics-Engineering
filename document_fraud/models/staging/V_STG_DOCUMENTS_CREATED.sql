{{ config(materialized='view') }}


with DOCUMENT_SUBTYPE_TREATMENT as (
select 
    dc.id as event_id, 
    dc.data_eventdate as receipt_at,
    dc.time as issue_at,
    dc.data_id_id as document_id,
    dc.data_acquisitionid_organizationid as channel_acquisition_id,
    lower(trim(dc.data_acquisitionid_type)) as acquisition_type, 
    lower(trim(dc.data_documentclass_documentsubtype)) as document_subtype,
    dc.data_documentclass_documenttype as document_category,
   -- country treatment 
   case when dc.data_documentclass_documentsubtype like '%ADDRESS_PROOF%'
          or dc.data_documentclass_documentsubtype like '%BANK_ACCOUNT_DETAILS%'
          or dc.data_documentclass_documentsubtype like '%BANK_ACCOUNT_STATEMENT%'
          or dc.data_documentclass_documentsubtype like '%INCOME_TAX_DECLARATION_STATEMENT%'
          or dc.data_documentclass_documentsubtype like '%INCOME_TAX_STATEMENT%'
          or dc.data_documentclass_documentsubtype like '%PAYSLIP%'
          or dc.data_documentclass_documentsubtype like '%VEHICLE_INSURANCE_INFORMATION_STATEMENT%'
          or dc.data_documentclass_documentsubtype like '%VEHICLE_REGISTRATION_CERTIFICATE%'
        then right(dc.data_documentclass_documentsubtype, 3)
        when dc.data_documentclass_documentsubtype like '%DRIVING_LICENSE%' 
          and dc.data_documentclass_documentsubtype like '%CARD%' 
        then left(right(dc.data_documentclass_documentsubtype, 13), 3)
        when dc.data_documentclass_documentsubtype like '%DRIVING_LICENSE%' 
          and dc.data_documentclass_documentsubtype like '%PAPER%' 
        then left(right(dc.data_documentclass_documentsubtype, 14), 3)
        when dc.data_documentclass_documentsubtype like '%DRIVING_LICENSE_%' 
          and dc.data_documentclass_documentsubtype not like '%XYZ%' 
        then left(right(dc.data_documentclass_documentsubtype, 8), 3)
        when dc.data_documentclass_documentsubtype like '%XYZ%' 
        then 'XYZ'
        when dc.data_documentclass_documentsubtype like '%ID_CARD%' 
        then left(right(dc.data_documentclass_documentsubtype, 8), 3)     
        when dc.data_documentclass_documentsubtype like '%PASSPORT%' 
          and dc.data_documentclass_documentsubtype not like '%XYZ%' 
        then left(right(dc.data_documentclass_documentsubtype, 8), 3)   
        when dc.data_documentclass_documentsubtype like '%RESIDENCE_PERMIT%' 
          and dc.data_documentclass_documentsubtype not like '%XYZ%' 
        then left(right(dc.data_documentclass_documentsubtype, 8), 3)   
   else null end as document_issuing_country,
   -- date treatment
   case when (dc.data_documentclass_documentsubtype like '%RESIDENCE_PERMIT%' 
          or dc.data_documentclass_documentsubtype like '%PASSPORT%' 
          or dc.data_documentclass_documentsubtype like '%ID_CARD%' 
          or dc.data_documentclass_documentsubtype like '%DRIVING_LICENSE%') 
         and dc.data_documentclass_documentsubtype not like '%XYZ%' 
        then right(dc.data_documentclass_documentsubtype, 4)
   else null end as document_issue_year
--from public.test_technique_ae_document_created dc
from {{source('CREATION', 'test_technique_ae_document_created')}} dc  
where 1 = 1
)
select 
    ds.event_id, 
    ds.receipt_at,
    ds.issue_at,
    ds.document_id,
    ds.channel_acquisition_id,
    ds.acquisition_type, 
    ds.document_subtype,
    lower(trim(ds.document_category)) as document_category,
    lower(trim(ds.document_issuing_country)) as document_issuing_country,
    lower(trim(ds.document_issue_year)) as document_issue_year
from DOCUMENT_SUBTYPE_TREATMENT ds
where 1 = 1
