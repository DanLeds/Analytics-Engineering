#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="${PROJECT_DIR}/logs"
LOG_FILE="${LOG_DIR}/daily_maintenance_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$LOG_DIR"

log() {
    local level="$1"
    shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*" | tee -a "$LOG_FILE"
}

log_info() { log "INFO" "$@"; }
log_error() { log "ERROR" "$@"; }
log_success() { log "SUCCESS" "$@"; }

cleanup() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_error "Script failed with exit code $exit_code"
    fi
    exit $exit_code
}

trap cleanup EXIT

cd "$PROJECT_DIR" || { log_error "Cannot cd to project directory: $PROJECT_DIR"; exit 1; }

log_info "Starting daily maintenance for document_fraud project"
log_info "Project directory: $PROJECT_DIR"
log_info "Log file: $LOG_FILE"

log_info "Step 1/4: Running nightly models..."
if dbt run --select tag:nightly_1 2>&1 | tee -a "$LOG_FILE"; then
    log_success "Models executed successfully"
else
    log_error "Model execution failed"
    exit 1
fi

log_info "Step 2/4: Running migration (create/update infrastructure)..."
if dbt run-operation run_migration 2>&1 | tee -a "$LOG_FILE"; then
    log_success "Migration completed successfully"
else
    log_error "Migration failed"
    exit 1
fi

log_info "Step 3/4: Verifying migration..."
if dbt run-operation verify_migration 2>&1 | tee -a "$LOG_FILE"; then
    log_success "Migration verification passed"
else
    log_error "Migration verification failed"
    exit 1
fi

log_info "Step 4/4: Archiving results..."
if dbt run-operation archive_results_control 2>&1 | tee -a "$LOG_FILE"; then
    log_success "Archive completed successfully"
else
    log_error "Archive failed"
    exit 1
fi

log_info "Step 5/5: Running data quality tests..."
if dbt test --select tag:nightly_1 2>&1 | tee -a "$LOG_FILE"; then
    log_success "All tests passed"
else
    log_error "Some tests failed - check log for details"
    exit 1
fi

log_success "Daily maintenance completed successfully"
log_info "Total log size: $(du -h "$LOG_FILE" | cut -f1)"

find "$LOG_DIR" -name "daily_maintenance_*.log" -mtime +30 -delete 2>/dev/null || true
log_info "Old logs cleaned up (>30 days)"
