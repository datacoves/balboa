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

## dbt Docs v2: A Fresh Look + Upsells

### What's New in dbt Docs v2

| Feature | Description |
|---------|-------------|
| Redesigned UI | Modern, clean interface with better navigation |
| Dark & Light Mode | Choose your preferred theme |
| Parquet-powered | Faster loading, scales to large projects |
| REST API | Query metadata at `/api/v1/` for AI agents & MCP |

### Built-in Fusion/Cloud Promotions

The new docs UI includes dismissible banners for paid features:

| Promotion | Message |
|-----------|---------|
| dbt State | "Stop rebuilding models that haven't changed" |
| Column-level lineage | "See exactly where each column comes from" |
| Collaborate with teams | "See every project in your org" |

### The Takeaway

dbt Docs v2 is a genuine improvement: faster, prettier, more capable. It includes promo banners for Fusion/Cloud features, but they're dismissible - so you can enjoy the new UI without the upsells.

> **dbt Docs Alternative: Tributary Docs by Datacoves** — Flat pricing. Deploy in your cloud. [tributarydocs.com](https://tributarydocs.com)
