# Setup

Install dependencies

```
pip install -r requirements.txt
```

Add this to dbt_project.yml
```
# Added for Elementary support
flags:
  require_explicit_package_overrides_for_builtin_materializations: False
  source_freshness_run_project_hooks: True
```

Add this to packages.yml (check for latest version)
```
  - package: elementary-data/elementary
    version: 0.16.2
```

# Set up elementary models
```
dbt deps
dbt run --select elementary
```

# Run dbt tests
`dbt test`

# Generate elementary report
`./update_profiles.py`
`edr report`

# View Elemenrary Report
`./e`
