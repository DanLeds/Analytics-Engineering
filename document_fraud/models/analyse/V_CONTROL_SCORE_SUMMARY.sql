{{ config(
    materialized='view',
    tags=["views"],
    persist_docs={"relation": true, "columns": true}
) }}


SELECT 
    control_score,
    SUM(document_count) AS total_documents,
    SUM(CASE WHEN control_status = 'OK' THEN document_count ELSE 0 END) AS count_ok,
    SUM(CASE WHEN control_status = 'KO' THEN document_count ELSE 0 END) AS count_ko,
    SUM(CASE WHEN control_status = 'NA' THEN document_count ELSE 0 END) AS count_na,
    SUM(CASE WHEN control_status IS NULL THEN document_count ELSE 0 END) AS count_null,
    -- Pourcentages
    ROUND(100.0 * SUM(CASE WHEN control_status = 'OK' THEN document_count ELSE 0 END) / 
          GREATEST(SUM(document_count), 1), 2) AS perc_ok,
    ROUND(100.0 * SUM(CASE WHEN control_status = 'KO' THEN document_count ELSE 0 END) / 
          GREATEST(SUM(document_count), 1), 2) AS perc_ko,
    MAX(snapshot_date) AS last_update
FROM {{ ref('RESULTS_CONTROL_SCORE_2') }}
WHERE 1 = 1 
    and snapshot_date = (SELECT MAX(snapshot_date) FROM {{ ref('RESULTS_CONTROL_SCORE_2') }})
GROUP BY control_score