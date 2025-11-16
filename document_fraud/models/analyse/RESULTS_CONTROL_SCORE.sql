{{ config(
    materialized='table',
    tags=["nightly_1"], 
    labels={
      "table_type": "fact_table"
    },
    indexes=[
        {'columns': ['control_score'], 'type': 'btree'},
        {'columns': ['last_issue_date'], 'type': 'brin'},
        {'columns': ['snapshot_date'], 'type': 'brin'}
    ],
    post_hook=[
        "ANALYZE {{ this }}",
        "ALTER TABLE {{ this }} SET (autovacuum_vacuum_scale_factor = 0.1)",
        "ALTER TABLE {{ this }} SET (fillfactor = 90)",
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_metrics_json ON {{ this }} USING GIN (metrics_json)"
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
    FROM {{ref('V_STG_DOCUMENTS_CONTROLLED')}} co
    --from analytics."V_STG_DOCUMENTS_CONTROLLED" co
    LEFT JOIN {{ref('V_STG_DOCUMENTS_CREATED')}} cr
    --left join analytics."V_STG_DOCUMENTS_CREATED" cr
        USING(document_id)
    where 1 = 1 
        and co.issue_at::date >= CURRENT_DATE - INTERVAL '730 days'
    ORDER BY co.document_id, co.control_score, co.issue_at DESC
),
-- Agrégations avec FILTER pour PostgreSQL
AGGREGATED_METRICS AS (
    SELECT 
        control_score,
        COUNT(DISTINCT document_id) AS total_documents,
        COUNT(DISTINCT document_id) FILTER (WHERE control_status = 'OK') AS count_ok,
        COUNT(DISTINCT document_id) FILTER (WHERE control_status = 'KO') AS count_ko,
        COUNT(DISTINCT document_id) FILTER (WHERE control_status = 'NA') AS count_na,
        COUNT(DISTINCT document_id) FILTER (WHERE control_status IS NULL) AS count_null,
        -- Stats simples avec fonctions standards
        MAX(issue_at)::DATE AS last_issue_date,
        MIN(issue_at)::DATE AS first_issue_date,
        COUNT(DISTINCT document_category) AS unique_categories,
        COUNT(DISTINCT document_issuing_country) AS unique_countries
    FROM BASE
    where 1 = 1
    GROUP BY control_score
),
FINAL_RESULT AS (
    SELECT 
        control_score,
        total_documents,
        count_ok,
        count_ko,
        count_na,
        count_null,
        -- Calcul des pourcentages sécurisé
        ROUND(100.0 * count_ok / GREATEST(total_documents, 1), 2) AS perc_ok,
        ROUND(100.0 * count_ko / GREATEST(total_documents, 1), 2) AS perc_ko,
        ROUND(100.0 * count_na / GREATEST(total_documents, 1), 2) AS perc_na,
        ROUND(100.0 * count_null / GREATEST(total_documents, 1), 2) AS perc_null,
        -- Informations temporelles
        first_issue_date,
        last_issue_date,
        last_issue_date - first_issue_date AS days_span,
        -- Métadonnées
        unique_categories,
        unique_countries,
        NOW() AT TIME ZONE 'UTC' AS created_at,
        CURRENT_DATE AS snapshot_date,
        -- Clé unique
        MD5(control_score || '|' || CURRENT_DATE::text) AS unique_key,
        -- JSONB pour métriques flexibles
        JSONB_BUILD_OBJECT(
            'counts', JSONB_BUILD_OBJECT(
                'ok', count_ok,
                'ko', count_ko,
                'na', count_na,
                'null', count_null,
                'total', total_documents
            ),
            'percentages', JSONB_BUILD_OBJECT(
                'ok_pct', ROUND(100.0 * count_ok / GREATEST(total_documents, 1), 2),
                'ko_pct', ROUND(100.0 * count_ko / GREATEST(total_documents, 1), 2),
                'na_pct', ROUND(100.0 * count_na / GREATEST(total_documents, 1), 2),
                'null_pct', ROUND(100.0 * count_null / GREATEST(total_documents, 1), 2)
            ),
            'metadata', JSONB_BUILD_OBJECT(
                'first_date', first_issue_date,
                'last_date', last_issue_date,
                'days_span', last_issue_date - first_issue_date,
                'unique_categories', unique_categories,
                'unique_countries', unique_countries
            )
        ) AS metrics_json
    FROM AGGREGATED_METRICS
    where 1 = 1
)
SELECT 
    control_score,
    total_documents,
    count_ok,
    perc_ok,
    count_ko,
    perc_ko,
    count_na,
    perc_na,
    count_null,
    perc_null,
    first_issue_date,
    last_issue_date,
    days_span,
    unique_categories,
    unique_countries,
    metrics_json,
    created_at,
    snapshot_date,
    unique_key
FROM FINAL_RESULT
where 1 = 1
ORDER BY control_score 