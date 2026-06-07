# Install dbt 2.0
```
pip install -U dbt-core==2.0.0a1
```

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
uvx --from git+https://github.com/dbt-labs/dbt-autofix.git dbt-autofix deprecations
```

# Fix remaining issues
```bash
dbt compile
```
fix all errors and warnings, you can remove the dynamic table materialization

# dbt 2.0 & Fusion Overview

## dbt Core 2.0: Open Source vs Proprietary Features

Proprietary features require a dbt Labs contract.

| Feature | Open Source | Proprietary |
|---------|:-----------:|:-----------:|
| Rust parser & compiler | ✅ | |
| Parquet metadata storage | ✅ | |
| New dbt-docs UI | ✅ | |
| Node-level lineage (DAG) | ✅ | |
| ADBC connections | ✅ | |
| Column-level lineage | | ✅ |
| Column impact analysis | | ✅ |
| SQL schema inference | | ✅ |
| Unit tests before build | | ✅ |
| dbt State (cloud sync) | | ✅ |
| SQL linting & static analysis | | ✅ |

## dbt Core v2 vs Fusion: What's the difference?

| | dbt Core v2 | dbt Fusion |
|---|---|---|
| License | Apache 2.0 Open Source | Proprietary license |
| Runtime | Rust-based runtime | Same Rust foundation |
| Pricing | Free forever | Free tier + premium |
| VS Code extension | No VS Code extension | VS Code extension included |

### What Fusion Adds

| Feature | Description |
|---------|-------------|
| SQL Comprehension | Native understanding across dialects, catches errors instantly |
| Column-Level Lineage | Trace model & column definitions across your project |
| Smart Parallelism | Auto-optimizes threads per adapter (Snowflake, Databricks, etc.) |
| Inline CTE Preview | Faster debugging with real-time SQL previews |

### VS Code Extension

Requires Fusion (not compatible with Core). 14-day free trial · Most features free after · Works in Cursor & Windsurf too.
