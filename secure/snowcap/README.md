# Snowflake Infrastructure as Code

This project uses [Snowcap](https://github.com/datacoves/snowcap) to manage all Snowflake infrastructure declaratively—databases, schemas, warehouses, roles, grants, and more.

## Quick Start

```bash
# Preview changes
./plan.sh

# Apply changes
./apply.sh
```

## Security Model: Modular RBAC

We use a **composable role architecture** where atomic "building block" roles are assembled into functional roles that users are assigned to.

```
┌─────────────────────────────────────────────────────────────┐
│                    FUNCTIONAL ROLES                         │
│            (analyst, loader, transformer_dbt)               │
│                  Users are assigned here                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ inherits
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 COMPOSITE ROLES (optional)                  │
│                    (z_base__analyst)                        │
│              Groups atomic roles for a persona              │
└──────────────────────────┬──────────────────────────────────┘
                           │ inherits
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      ATOMIC ROLES                           │
│  z_db__<database>           → USAGE on database             │
│  z_schema__<schema>         → USAGE on schema               │
│  z_wh__<warehouse>          → USAGE + MONITOR on warehouse  │
│  z_stage__<stage>__read     → READ on stage                 │
│  z_stage__<stage>__write    → READ + WRITE on stage         │
│  z_tables_views__select     → SELECT on all tables/views    │
│  z_account__create_database → CREATE DATABASE on account    │
└─────────────────────────────────────────────────────────────┘
```

### Naming Convention

All atomic roles use the pattern: `z_<type>__<object_name>`

| Prefix | Grants | Example |
|--------|--------|---------|
| `z_db__` | USAGE on database | `z_db__balboa` |
| `z_schema__` | USAGE on schema | `z_schema__l1_loans` |
| `z_wh__` | USAGE + MONITOR on warehouse | `z_wh__wh_transforming` |
| `z_stage__` | READ or WRITE on stage | `z_stage__raw__dbt_artifacts__artifacts__read` |
| `z_account__` | Account-level privileges | `z_account__create_database` |
| `z_tables_views__` | SELECT on tables/views | `z_tables_views__select` |

### Functional Roles

| Role | Purpose | Key Permissions |
|------|---------|-----------------|
| `analyst` | Query data, build reports | SELECT on all layers, warehouse access |
| `loader` | Ingest data into RAW | Write to RAW database, loading warehouse |
| `transformer_dbt` | Transform data with dbt | All databases, create schemas, orchestration warehouse |

## Project Structure

```
resources/
├── account.yml              # Account-level settings
├── databases.yml            # Database definitions + shared DB roles
├── schemas.yml              # Schema list (auto-generates z_schema__ roles)
├── warehouses.yml           # Warehouse list (auto-generates z_wh__ roles)
├── stages.yml               # Stage definitions + z_stage__ roles
├── users.yml                # User definitions
├── roles__base.yml          # Atomic privilege roles (non-templated)
├── roles__functional.yml    # Functional roles + role_grants
│
└── object_templates/        # Templates that auto-generate resources
    ├── database.yml         # Creates databases + z_db__ roles
    ├── schema.yml           # Creates schemas + z_schema__ roles
    └── warehouses.yml       # Creates warehouses + z_wh__ roles
```

## Common Tasks

### Add a New Schema

1. Add the schema to `schemas.yml`:

```yaml
- name: BALBOA.L3_NEW_ANALYTICS
```

2. Grant access by adding the auto-generated role to a functional role in `roles__functional.yml`:

```yaml
- to_role: z_base__analyst
  roles:
    - z_schema__l3_new_analytics  # Add this line
```

3. Run `./plan.sh` to preview, then `./apply.sh` to deploy.

### Add a New Database

1. Add the database to the `databases` list in `databases.yml`:

```yaml
- name: new_db
  owner: transformer_dbt
  max_data_extension_time_in_days: 10
```

2. The template auto-creates:
   - The database
   - `z_db__new_db` role with USAGE grant

3. Add `z_db__new_db` to the appropriate functional role in `roles__functional.yml`.

### Add a New Warehouse

1. Add the warehouse to the `warehouses` list in `warehouses.yml`:

```yaml
- name: wh_reporting
  size: medium
  auto_suspend: 120
```

2. The template auto-creates:
   - The warehouse
   - `z_wh__wh_reporting` role with USAGE + MONITOR grants

3. Add `z_wh__wh_reporting` to the appropriate functional role.

### Add a New User

Add the user to `users.yml`:

```yaml
users:
  - name: jane_doe
    email: jane@example.com
    default_role: analyst
```

## Why This Pattern?

| Benefit | Description |
|---------|-------------|
| **Auditable** | Each atomic role does exactly one thing—easy to audit |
| **DRY** | Templates auto-generate roles when you add resources |
| **Least Privilege** | Users only get functional roles, never atomic roles |
| **Composable** | Create new personas by mixing atomic roles |
| **Git-native** | Full change history via version control |

## Learn More

- [Snowcap GitHub](https://github.com/datacoves/snowcap)
