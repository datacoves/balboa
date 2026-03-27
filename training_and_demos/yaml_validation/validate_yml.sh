#!/usr/bin/env bash
# Validate dbt YAML files for formatting and schema issues
# Usage: ./validate_yml.sh <file_or_directory>

set -euo pipefail

SCHEMA_URL="https://raw.githubusercontent.com/dbt-labs/dbt-jsonschema/main/schemas/latest/dbt_yml_files-latest.json"
YAMLLINT_CONFIG="/config/workspace/.yamllint"

if [ $# -eq 0 ]; then
    echo "Usage: $0 <file_or_directory>"
    echo "Example: $0 transform/models/L1_inlets/loans/stg_personal_loans.yml"
    exit 1
fi

TARGET="$1"

echo "=== YAML Lint ==="
yamllint -c "$YAMLLINT_CONFIG" "$TARGET" || true

echo ""
echo "=== Schema Validation ==="
check-jsonschema --schemafile "$SCHEMA_URL" "$TARGET" 2>&1 \
    | grep -v "RequestsDependencyWarning" \
    | grep -v "warnings.warn" \
    | sed "s/'config', //g; s/, 'config'//g" \
    | grep -v "'config' was unexpected" \
    || true
