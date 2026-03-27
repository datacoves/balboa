# dbt YAML Validation

Validate dbt YAML files for formatting issues and schema errors.

## Install Dependencies

```bash
pip install yamllint check-jsonschema
```

## Manual Usage

### YAML Lint (catches formatting/indentation issues)

```bash
yamllint -c /config/workspace/.yamllint <file_or_directory>
```

Example:

```bash
yamllint -c /config/workspace/.yamllint transform/models/L1_inlets/loans/stg_personal_loans.yml
```

### Schema Validation (catches misspelled keys and invalid properties)

```bash
check-jsonschema --schemafile https://raw.githubusercontent.com/dbt-labs/dbt-jsonschema/main/schemas/latest/dbt_yml_files-latest.json <file> 2>&1 \
    | grep -v "RequestsDependencyWarning" \
    | grep -v "warnings.warn" \
    | sed "s/'config', //g; s/, 'config'//g" \
    | grep -v "'config' was unexpected"
```

> **Note:** The `config` property is valid in dbt but not yet supported in the JSON schema ([dbt-labs/dbt-jsonschema#213](https://github.com/dbt-labs/dbt-jsonschema/issues/213)), so we filter out those false positives.

## Script Usage

Run both checks at once:

```bash
./validate_yml.sh <file_or_directory>
```

Example:

```bash
./validate_yml.sh /config/workspace/transform/models/L1_inlets/loans/stg_personal_loans.yml
```
