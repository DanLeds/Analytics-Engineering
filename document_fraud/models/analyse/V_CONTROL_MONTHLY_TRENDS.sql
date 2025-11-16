{{ config(
    materialized='view',
    tags=["nightly_1"], 
    labels={
      "table_type": "aggregate_table"
    }
) }}


WITH monthly_data AS (
    SELECT 
        DATE_TRUNC('month', co.issue_at::timestamptz) AS issue_month,
        COALESCE(co.control_score, 'controlna') AS control_score,
        co.control_status,
        COUNT(DISTINCT co.document_id) AS document_count
    FROM {{ref('V_STG_DOCUMENTS_CONTROLLED')}} co
    --from analytics."V_STG_DOCUMENTS_CONTROLLED" co
    WHERE 1 = 1 
        AND co.issue_at::timestamptz >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY 1, 2, 3
),
monthly_summary AS (
    SELECT 
        issue_month,
        control_score,
        SUM(document_count) AS total_count,
        SUM(document_count) FILTER (WHERE control_status = 'OK') AS count_ok,
        SUM(document_count) FILTER (WHERE control_status = 'KO') AS count_ko,
        SUM(document_count) FILTER (WHERE control_status = 'NA') AS count_na,
        SUM(document_count) FILTER (WHERE control_status IS NULL) AS count_null
    FROM monthly_data
    WHERE 1 = 1
    GROUP BY issue_month, control_score
)
SELECT 
    issue_month::date as issue_month_date,
    control_score,
    total_count,
    count_ok,
    count_ko,
    count_na,
    count_null,
    ROUND(100.0 * count_ok / GREATEST(total_count, 1), 2) AS perc_ok,
    ROUND(100.0 * count_ko / GREATEST(total_count, 1), 2) AS perc_ko,
    -- Evolution mois sur mois
    LAG(total_count, 1) OVER (PARTITION BY control_score ORDER BY issue_month) AS prev_month_count,
    total_count - LAG(total_count, 1) OVER (PARTITION BY control_score ORDER BY issue_month) AS mom_change,
    ROUND(100.0 * (total_count - LAG(total_count, 1) OVER (PARTITION BY control_score ORDER BY issue_month)) 
          / GREATEST(LAG(total_count, 1) OVER (PARTITION BY control_score ORDER BY issue_month), 1), 2) AS mom_growth_pct
FROM monthly_summary
WHERE 1 = 1
ORDER BY control_score, issue_month DESC 