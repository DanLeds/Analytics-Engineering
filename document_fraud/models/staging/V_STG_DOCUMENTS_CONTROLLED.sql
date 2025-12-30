{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='sync_all_columns',
    indexes=[
        {'columns': ['event_id']},
        {'columns': ['document_id']},
        {'columns': ['control_name']},
        {'columns': ['receipt_at']},
    ]
) }}

with base as (
    select
        dc.id as event_id,
        dc.data_documentcontrolsdata_status as control_applied_status,
        dc.data_eventdate as receipt_at,
        dc.doc_id as document_id,
        dc.data_score as control_score,
        dc.time as issue_at,
        dc.data_documentcontrolsdata_documentcontrols_anonymized #>> '{0, status}' as control_status,
        dc.data_documentcontrolsdata_documentcontrols_anonymized #>> '{0, scores}' as control_scores,
        dc.data_documentcontrolsdata_documentfaceids #>> '{0, id}' as document_face_id,
        dc.data_documentcontrolsdata_documentfaceids #>> '{0, type}' as document_type,
        dc.data_documentcontrolsdata_documentfaceids #>> '{0, domain}' as control_domain,
        dc.data_documentcontrolsdata_documentfaceids #>> '{0, organizationId}' as channel_acquisition_id,
        lower(trim(dc.data_documentcontrolsdata_documentcontrols_anonymized #>> '{0, identifier}')) as control_identifier
    from {{ source('CONTROLS', 'test_technique_ae_document_controlled') }} dc
    where 1 = 1
    {% if is_incremental() %}
        and dc.data_eventdate > (select max(receipt_at) from {{ this }})
    {% endif %}
)

select
    event_id,
    control_applied_status,
    receipt_at,
    document_id,
    control_score,
    issue_at,
    control_status,
    control_scores,
    document_face_id,
    document_type,
    control_domain,
    channel_acquisition_id,
    control_identifier,
    case
        when position('document' in control_identifier) > 0 then 'document'
        when position('applicant' in control_identifier) > 0 then 'applicant'
        else null
    end as control_subject,
    case
        when position('alternatefamilynameconsistency' in control_identifier) > 0 then 'alternate_family_name_consistency'
        when position('alternatefamilynamedetection' in control_identifier) > 0 then 'alternate_family_name_detection'
        when position('familynameaccountholdersearch' in control_identifier) > 0 then 'family_name_account_holder_search'
        when position('familynameconsistency' in control_identifier) > 0 then 'family_name_consistency'
        when position('familynamesearch' in control_identifier) > 0 then 'family_name_search'
        when position('addresslocalityconsistency' in control_identifier) > 0 then 'address_locality_consistency'
        when position('addressconsistency' in control_identifier) > 0 then 'address_consistency'
        when position('birthdatemrzconsistency' in control_identifier) > 0 then 'birthdate_mrz_consistency'
        when position('personalidconsistency' in control_identifier) > 0 then 'personal_id_consistency'
        when position('bicibanconsistency' in control_identifier) > 0 then 'bic_iban_consistency'
        when position('datevalidation' in control_identifier) > 0 then 'date_validation'
        else null
    end as control_name
from base
