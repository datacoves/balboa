# Install dbt fusion

```bash
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
```

# Update dbt deps

```bash
rm package-lock.yml
dbt deps
```

# Fix configs

Use [dbt tool](https://github.com/dbt-labs/dbt-autofix) to fix deprecations

```bash
uvx --from git+https://github.com/dbt-labs/dbt-autofix.git dbt-autofix deprecations --include-packages
```

# Fix remaining issues
```
run dbt compile
fix all errors and warnings, you can remove the dynamic table materialization
```
