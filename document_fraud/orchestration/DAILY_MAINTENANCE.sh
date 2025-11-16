#!/bin/bash
# Script d'orchestration pour maintenance quotidienne

# Exécuter les modèles
dbt run --models control_metrics

# Créer les structures si nécessaire
dbt run-operation create_history_table
dbt run-operation create_monthly_partitions

# Maintenance
dbt run-operation maintain_control_metrics
