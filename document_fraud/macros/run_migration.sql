-- =====================================================
-- Fichier: macros/run_migration.sql 
-- Adapté pour le modèle RESULTS_CONTROL_SCORE_2
-- =====================================================

{% macro run_migration() %}
  {{ log("Début de la migration de l'infrastructure d'historique pour RESULTS_CONTROL_SCORE_2...", info=True) }}
  
  -- 1. Création de la table d'historique
  {% set create_history_table %}
    CREATE TABLE IF NOT EXISTS {{ target.schema }}.results_control_score_2_history (
        control_score TEXT NOT NULL,
        control_status TEXT NOT NULL DEFAULT 'NULL',
        document_count INTEGER,
        total_documents INTEGER,
        percentage NUMERIC(5,2),
        first_issue_date DATE,
        last_issue_date DATE,
        days_span INTEGER,
        unique_categories INTEGER,
        unique_countries INTEGER,
        unique_control_names INTEGER,
        unique_channels INTEGER,
        metrics_json JSONB,
        created_at TIMESTAMPTZ NOT NULL,
        snapshot_date DATE NOT NULL,
        unique_key TEXT,
        CONSTRAINT pk_results_control_score_2_history 
            PRIMARY KEY (control_score, control_status, snapshot_date)
    ) PARTITION BY RANGE (snapshot_date)
  {% endset %}
  
  {% do run_query(create_history_table) %}
  {{ log("✅ Table d'historique créée", info=True) }}
  
  -- 2. Création des partitions
  {% set create_partitions %}
    DO $$
    DECLARE
        partition_date DATE;
        partition_name TEXT;
        sql_create TEXT;
    BEGIN
        -- Créer les partitions pour les 12 derniers mois et 6 mois futurs
        FOR partition_date IN 
            SELECT generate_series(
                DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months'),
                DATE_TRUNC('month', CURRENT_DATE + INTERVAL '6 months'),
                '1 month'::interval
            )::DATE
        LOOP
            partition_name := 'results_control_score_2_history_' || TO_CHAR(partition_date, 'YYYY_MM');
            
            -- Vérifier si la partition existe
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = partition_name
                AND n.nspname = '{{ target.schema }}'
            ) THEN
                sql_create := format(
                    'CREATE TABLE IF NOT EXISTS {{ target.schema }}.%I PARTITION OF {{ target.schema }}.results_control_score_2_history 
                    FOR VALUES FROM (%L) TO (%L)',
                    partition_name,
                    partition_date,
                    partition_date + INTERVAL '1 month'
                );
                
                EXECUTE sql_create;
                RAISE NOTICE 'Partition créée: %', partition_name;
            END IF;
        END LOOP;
    END $$
  {% endset %}
  
  {% do run_query(create_partitions) %}
  {{ log("✅ Partitions créées", info=True) }}
  
  -- 3. Création des index
  {% set create_indexes %}
    -- Index sur la table d'historique
    CREATE INDEX IF NOT EXISTS idx_results_history_snapshot_date 
        ON {{ target.schema }}.results_control_score_2_history (snapshot_date);
    
    CREATE INDEX IF NOT EXISTS idx_results_history_control_score 
        ON {{ target.schema }}.results_control_score_2_history (control_score);
    
    CREATE INDEX IF NOT EXISTS idx_results_history_control_status 
        ON {{ target.schema }}.results_control_score_2_history (control_status);
    
    -- Index GIN sur la table principale RESULTS_CONTROL_SCORE_2 (si elle existe)
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE LOWER(c.relname) = 'results_control_score_2'
            AND n.nspname = '{{ target.schema }}'
        ) THEN
            CREATE INDEX IF NOT EXISTS idx_results_control_score_2_json 
                ON {{ target.schema }}."RESULTS_CONTROL_SCORE_2" USING GIN (metrics_json);
        END IF;
    END $$
  {% endset %}
  
  {% do run_query(create_indexes) %}
  {{ log("✅ Index créés", info=True) }}
  
  -- 4. Création des vues utilitaires
  {% set create_views %}
    -- Vue pour le dernier snapshot
    CREATE OR REPLACE VIEW {{ target.schema }}.v_results_control_latest AS
    SELECT * 
    FROM {{ target.schema }}."RESULTS_CONTROL_SCORE_2"
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) 
        FROM {{ target.schema }}."RESULTS_CONTROL_SCORE_2"
    );
    
    -- Vue résumée par control_score (agrégation des statuts)
    CREATE OR REPLACE VIEW {{ target.schema }}.v_results_control_summary AS
    SELECT 
        control_score,
        SUM(document_count) AS total_documents,
        SUM(CASE WHEN control_status = 'OK' THEN document_count ELSE 0 END) AS count_ok,
        SUM(CASE WHEN control_status = 'KO' THEN document_count ELSE 0 END) AS count_ko,
        SUM(CASE WHEN control_status = 'NA' THEN document_count ELSE 0 END) AS count_na,
        SUM(CASE WHEN control_status IS NULL OR control_status = 'NULL' THEN document_count ELSE 0 END) AS count_null,
        ROUND(100.0 * SUM(CASE WHEN control_status = 'OK' THEN document_count ELSE 0 END) / 
              GREATEST(SUM(document_count), 1), 2) AS perc_ok,
        ROUND(100.0 * SUM(CASE WHEN control_status = 'KO' THEN document_count ELSE 0 END) / 
              GREATEST(SUM(document_count), 1), 2) AS perc_ko,
        MAX(snapshot_date) AS last_snapshot
    FROM {{ target.schema }}."RESULTS_CONTROL_SCORE_2"
    WHERE snapshot_date = (
        SELECT MAX(snapshot_date) 
        FROM {{ target.schema }}."RESULTS_CONTROL_SCORE_2"
    )
    GROUP BY control_score
    ORDER BY control_score
  {% endset %}
  
  {% do run_query(create_views) %}
  {{ log("✅ Vues créées", info=True) }}
  
  {{ log("🎉 Migration terminée avec succès pour RESULTS_CONTROL_SCORE_2!", info=True) }}
  
{% endmacro %}

-- =====================================================
-- Macro de vérification
-- =====================================================

{% macro verify_migration() %}
  {{ log("Vérification de la migration pour RESULTS_CONTROL_SCORE_2...", info=True) }}
  
  {% set check_tables %}
    SELECT 
        'Table RESULTS_CONTROL_SCORE_2' as objet,
        EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = '{{ target.schema }}' 
            AND UPPER(table_name) = 'RESULTS_CONTROL_SCORE_2'
        ) as existe
    UNION ALL
    SELECT 
        'Table historique' as objet,
        EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = '{{ target.schema }}' 
            AND table_name = 'results_control_score_2_history'
        ) as existe
    UNION ALL
    SELECT 
        'Vue v_results_control_latest' as objet,
        EXISTS (
            SELECT 1 FROM information_schema.views 
            WHERE table_schema = '{{ target.schema }}' 
            AND table_name = 'v_results_control_latest'
        ) as existe
    UNION ALL
    SELECT 
        'Vue v_results_control_summary' as objet,
        EXISTS (
            SELECT 1 FROM information_schema.views 
            WHERE table_schema = '{{ target.schema }}' 
            AND table_name = 'v_results_control_summary'
        ) as existe
  {% endset %}
  
  {% set check_partitions %}
    SELECT 
        'Partitions (' || COUNT(*) || ')' as objet,
        COUNT(*) > 0 as existe
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = '{{ target.schema }}'
    AND c.relname LIKE 'results_control_score_2_history_%'
  {% endset %}
  
  {% if execute %}
    {% set results_table = run_query(check_tables) %}
    {% for row in results_table %}
      {{ log("  " ~ row[0] ~ ": " ~ ("✅ OK" if row[1] else "❌ MANQUANT"), info=True) }}
    {% endfor %}
    
    {% set results_parts = run_query(check_partitions) %}
    {% for row in results_parts %}
      {{ log("  " ~ row[0] ~ ": " ~ ("✅ OK" if row[1] else "❌ MANQUANT"), info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

-- =====================================================
-- Macro de rollback
-- =====================================================

{% macro rollback_migration() %}
  {{ log("⚠️  Suppression de l'infrastructure d'historique pour RESULTS_CONTROL_SCORE_2...", info=True) }}
  
  {% set drop_all %}
    DROP VIEW IF EXISTS {{ target.schema }}.v_results_control_latest CASCADE;
    DROP VIEW IF EXISTS {{ target.schema }}.v_results_control_summary CASCADE;
    DROP TABLE IF EXISTS {{ target.schema }}.results_control_score_2_history CASCADE;
  {% endset %}
  
  {% do run_query(drop_all) %}
  {{ log("✅ Infrastructure supprimée", info=True) }}
{% endmacro %}

-- =====================================================
-- Macro d'archivage des données
-- =====================================================

{% macro archive_results_control() %}
  {{ log("Archivage des données de RESULTS_CONTROL_SCORE_2...", info=True) }}
  
  {% set archive_sql %}
    INSERT INTO {{ target.schema }}.results_control_score_2_history
    SELECT 
        control_score,
        COALESCE(control_status, 'NULL') as control_status,
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
    FROM {{ target.schema }}."RESULTS_CONTROL_SCORE_2"
    WHERE snapshot_date = CURRENT_DATE
    ON CONFLICT (control_score, control_status, snapshot_date) 
    DO UPDATE SET
        document_count = EXCLUDED.document_count,
        total_documents = EXCLUDED.total_documents,
        percentage = EXCLUDED.percentage,
        metrics_json = EXCLUDED.metrics_json,
        created_at = EXCLUDED.created_at
  {% endset %}
  
  {% set result = run_query(archive_sql) %}
  {{ log("✅ Données archivées", info=True) }}
{% endmacro %}