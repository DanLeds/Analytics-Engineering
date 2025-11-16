{{ config(
    materialized='table',
    tags=["nightly_1"], 
    labels={
      "table_type": "fact_table"
    },
    indexes=[
        {'columns': ['control_score'], 'type': 'btree'},
        {'columns': ['control_status'], 'type': 'btree'},
        {'columns': ['created_at'], 'type': 'brin'},
        {'columns': ['control_score', 'control_status'], 'type': 'btree'},
        {'columns': ['snapshot_date'], 'type': 'brin'}
    ],
    post_hook=[
        "ANALYZE {{ this }}",
        "ALTER TABLE {{ this }} SET (autovacuum_vacuum_scale_factor = 0.1)",
        "ALTER TABLE {{ this }} SET (fillfactor = 90)"
    ]
) }}

WITH BASE AS ( 
    SELECT 
        DISTINCT ON (co.document_id, co.control_score)
        co.document_id,
        co.control_status,
        co.issue_at::timestamptz,
        COALESCE(co.control_score, 'controlna') AS control_score,
        co.control_name,
        co.channel_acquisition_id,
        cr.document_category, 
        cr.document_issuing_country,
        cr.document_issue_year
    --FROM {{ref('V_STG_DOCUMENTS_CONTROLLED')}} co 
    from analytics."V_STG_DOCUMENTS_CONTROLLED" co
    --LEFT JOIN {{ref('V_STG_DOCUMENTS_CREATED')}} cr
    left join analytics."V_STG_DOCUMENTS_CREATED" cr
        USING(document_id)
    WHERE 1 = 1 
    and co.issue_at::date >= CURRENT_DATE - INTERVAL '730 days'
    ORDER BY co.document_id, co.control_score, co.issue_at DESC
),
-- Agrégation par control_score ET control_status (modèle en ligne)
AGGREGATED_BY_STATUS AS (
    SELECT 
        control_score,
        control_status,
        COUNT(DISTINCT document_id) AS document_count,
        MAX(issue_at)::DATE AS last_issue_date,
        MIN(issue_at)::DATE AS first_issue_date,
        COUNT(DISTINCT document_category) AS unique_categories,
        COUNT(DISTINCT document_issuing_country) AS unique_countries,
        COUNT(DISTINCT control_name) AS unique_control_names,
        COUNT(DISTINCT channel_acquisition_id) AS unique_channels
    FROM BASE
    GROUP BY control_score, control_status
),
-- Calcul des totaux par control_score pour les pourcentages
TOTALS_BY_SCORE AS (
    SELECT 
        control_score,
        SUM(document_count) AS total_documents,
        MAX(last_issue_date) AS score_last_issue_date,
        MIN(first_issue_date) AS score_first_issue_date
    FROM AGGREGATED_BY_STATUS
    WHERE 1 = 1
    GROUP BY control_score
),
-- Résultat final en ligne
FINAL_RESULT AS (
    SELECT 
        a.control_score,
        a.control_status,
        a.document_count,
        t.total_documents,
        -- Pourcentage de ce statut par rapport au total du control_score
        ROUND(100.0 * a.document_count / GREATEST(t.total_documents, 1), 2) AS percentage,
        -- Dates
        a.first_issue_date,
        a.last_issue_date,
        a.last_issue_date - a.first_issue_date AS days_span,
        -- Métadonnées
        a.unique_categories,
        a.unique_countries,
        a.unique_control_names,
        a.unique_channels,
        -- Timestamps
        NOW() AT TIME ZONE 'UTC' AS created_at,
        CURRENT_DATE AS snapshot_date,
        -- Clé unique incluant control_status
        MD5(
            a.control_score || '|' || 
            COALESCE(a.control_status, 'NULL') || '|' || 
            CURRENT_DATE::text
        ) AS unique_key,
        -- JSONB pour stockage flexible
        JSONB_BUILD_OBJECT(
            'metrics', JSONB_BUILD_OBJECT(
                'document_count', a.document_count,
                'percentage', ROUND(100.0 * a.document_count / GREATEST(t.total_documents, 1), 2),
                'total_for_score', t.total_documents
            ),
            'dates', JSONB_BUILD_OBJECT(
                'first_date', a.first_issue_date,
                'last_date', a.last_issue_date,
                'days_span', a.last_issue_date - a.first_issue_date,
                'score_date_range', JSONB_BUILD_OBJECT(
                    'first', t.score_first_issue_date,
                    'last', t.score_last_issue_date
                )
            ),
            'dimensions', JSONB_BUILD_OBJECT(
                'categories', a.unique_categories,
                'countries', a.unique_countries,
                'control_names', a.unique_control_names,
                'channels', a.unique_channels
            )
        ) AS metrics_json
    FROM AGGREGATED_BY_STATUS a
    INNER JOIN TOTALS_BY_SCORE t 
        ON a.control_score = t.control_score
    WHERE 1 = 1
)
SELECT 
    control_score,
    control_status,
    document_count,
    total_documents,
    percentage,
    first_issue_date,
    last_issue_date,
    days_span,
    unique_categories,
    unique_countries,
    unique_control_names,
    unique_channels,
    metrics_json,
    created_at,
    snapshot_date,
    unique_key
FROM FINAL_RESULT
WHERE 1 = 1
ORDER BY 
    control_score,
    CASE 
        WHEN control_status = 'OK' THEN 1
        WHEN control_status = 'KO' THEN 2
        WHEN control_status = 'NA' THEN 3
        WHEN control_status IS NULL THEN 4
        ELSE 5
    END