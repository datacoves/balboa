# Multi-Team dbt Orchestration with Airflow

This guide explains a **Hybrid DAG** approach for orchestrating dbt models across multiple teams with shared dependencies on Core models, using Datacoves task decorators and Airflow Datasets.

## Problem Statement

When multiple teams share a dbt project:
- **Team_1** has models that depend only on Team_1 sources (can run independently)
- **Team_1** also has models that depend on Team_1 sources AND Core (must wait for Core)
- **Core** models are shared dependencies across teams

**Goal:** Maximize parallelism by running team-independent models immediately, while properly sequencing models that depend on Core.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 1 (Parallel - No Waiting)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────┐             │
│  │  Team_1 EL    │     │  Team_2 EL    │     │   Core EL     │             │
│  └───────┬───────┘     └───────┬───────┘     └───────┬───────┘             │
│          │                     │                     │                      │
│          ▼                     ▼                     ▼                      │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────┐             │
│  │ Team_1 Only   │     │ Team_2 Only   │     │  Core Only    │             │
│  │   Models      │     │   Models      │     │   Models      │             │
│  │ (No Core      │     │ (No Core      │     │               │             │
│  │  dependency)  │     │  dependency)  │     │               │             │
│  └───────────────┘     └───────────────┘     └───────┬───────┘             │
│                                                      │                      │
│                                              Produces Dataset:              │
│                                              CORE_COMPLETE                  │
├──────────────────────────────────────────────────────┼──────────────────────┤
│                         PHASE 2 (Triggered by Core Dataset)                 │
│                                                      │                      │
│                              ┌───────────────────────┘                      │
│                              ▼                                              │
│              ┌───────────────────────────────┐                              │
│              │     Airflow Dataset Trigger   │                              │
│              │   schedule=[CORE_COMPLETE]    │                              │
│              └───────────────┬───────────────┘                              │
│                              │                                              │
│              ┌───────────────┴───────────────┐                              │
│              ▼                               ▼                              │
│     ┌─────────────────┐            ┌─────────────────┐                     │
│     │ Team_1 + Core   │            │ Team_2 + Core   │                     │
│     │    Models       │            │    Models       │                     │
│     └─────────────────┘            └─────────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Model Selection Strategy: Dynamic Path-Based Selection

### Why Path-Based Instead of Tags?

| Approach | Pros | Cons |
|----------|------|------|
| **Tags** | Flexible, can cross-cut folders | Easy to forget, requires discipline |
| **Paths** | Enforced by directory structure, self-documenting | Requires team folder organization |

**Recommendation:** Use **path-based selectors with intersection** because:
- Directory structure is enforced by code review/PR process
- No risk of forgetting to tag new models
- dbt dynamically determines Core dependencies via the intersection operator
- No special folder structure needed (e.g., no `marts_with_core` folder)

### Folder Structure

Standard team-based organization - no special folders needed:

```
models/
├── core/
│   ├── staging/              # stg_* models for core sources
│   ├── intermediate/         # int_* models
│   └── marts/                # Core marts (shared across teams)
│
├── team_1/
│   ├── staging/              # Team 1 source staging
│   ├── intermediate/         # Team 1 intermediate models
│   └── marts/                # Team 1 marts (mix of independent & core-dependent)
│
├── team_2/
│   ├── staging/
│   ├── intermediate/
│   └── marts/
```

### The Intersection Operator

dbt's intersection operator (`,`) combined with the downstream operator (`+`) allows dynamic dependency detection:

- `path:models/core+` = Core models AND all their downstream dependents (anywhere in project)
- `path:models/team_1,path:models/core+` = Team_1 models that ARE downstream of Core

### dbt Selector Examples

```bash
# ============================================
# PHASE 1: Independent Models (run in parallel)
# ============================================

# Team 1 models that do NOT depend on Core
dbt build \
  --select path:models/team_1 \
  --exclude path:models/team_1,path:models/core+

# Team 2 models that do NOT depend on Core
dbt build \
  --select path:models/team_2 \
  --exclude path:models/team_2,path:models/core+

# Core models only
dbt build --select path:models/core

# ============================================
# PHASE 2: After Core completes
# ============================================

# Team 1 models that DO depend on Core
dbt build --select path:models/team_1,path:models/core+

# Team 2 models that DO depend on Core
dbt build --select path:models/team_2,path:models/core+
```

### How It Works

| Selector | Meaning |
|----------|---------|
| `path:models/team_1` | All models in team_1 folder |
| `path:models/core+` | Core models + all downstream dependents |
| `path:models/team_1,path:models/core+` | Intersection: team_1 models that are downstream of core |
| `--exclude path:models/team_1,path:models/core+` | Remove core-dependent team_1 models from selection |

**Key benefit:** When a developer adds `{{ ref('core_model') }}` to a team_1 model, it automatically moves to Phase 2 - no manual tagging or folder moves required.

### Alternative: Source-Based Selection

If you prefer selecting by source rather than path:

```bash
# Team_1 models NOT downstream of core sources
dbt build \
  --select source:team_1_source+ \
  --exclude source:team_1_source+,source:core_source+

# Team_1 models that ARE downstream of core sources
dbt build --select source:team_1_source+,source:core_source+
```

---

## Airflow Datasets for DAG Triggering

### What are Airflow Datasets?

Airflow Datasets enable **data-aware scheduling**:
- A DAG can **produce** a dataset (outlet)
- Another DAG can be **triggered** when that dataset is updated (inlet)
- No need for `ExternalTaskSensor` polling

### Benefits Over ExternalTaskSensor

| Feature | ExternalTaskSensor | Datasets |
|---------|-------------------|----------|
| Trigger mechanism | Polling (every N seconds) | Event-driven |
| Resource usage | Higher (continuous polling) | Lower (push-based) |
| Configuration | Need to know DAG/task IDs | Logical data names |
| Cross-DAG visibility | Limited | Native in Airflow UI |

---

## Implementation with Datacoves Decorators

### Step 1: Define Datasets

Create a central file for all dataset definitions:

```python
# orchestrate/utils/datasets.py
from airflow.datasets import Dataset

# EL completion datasets
EL_CORE_COMPLETE = Dataset("el://balboa/core/complete")
EL_TEAM_1_COMPLETE = Dataset("el://balboa/team_1/complete")
EL_TEAM_2_COMPLETE = Dataset("el://balboa/team_2/complete")

# dbt completion datasets
DBT_CORE_COMPLETE = Dataset("dbt://balboa/core/complete")
```

### Step 2: Core DAG (Producer)

```python
# orchestrate/dags/multi_team/L1_dbt_core.py
"""
## Core dbt Models
Builds all Core models and triggers downstream team DAGs via Dataset.
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from orchestrate.utils.datasets import EL_CORE_COMPLETE, DBT_CORE_COMPLETE


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Data Platform",
        owner_email="data-platform@example.com"
    ),
    description="Build Core dbt models",
    schedule=[EL_CORE_COMPLETE],  # Triggered when Core EL completes
    tags=["L1", "core", "dbt"],
)
def L1_dbt_core():

    @task.datacoves_dbt(
        connection_id="main_key_pair",
        outlets=[DBT_CORE_COMPLETE]  # Produces this dataset on success
    )
    def build_core_models():
        return "dbt build --select path:models/core"

    build_core_models()


L1_dbt_core()
```

### Step 3: Team Independent DAG (EL-Triggered)

```python
# orchestrate/dags/multi_team/L1_dbt_team_1_independent.py
"""
## Team 1 Independent Models
Builds Team 1 models that have NO dependency on Core.
Uses intersection operator to dynamically exclude core-dependent models.
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from orchestrate.utils.datasets import EL_TEAM_1_COMPLETE


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Team 1",
        owner_email="team1@example.com"
    ),
    description="Build Team 1 independent models (no Core dependency)",
    schedule=[EL_TEAM_1_COMPLETE],  # Triggered when Team 1 EL completes
    tags=["L1", "team_1", "dbt", "independent"],
)
def L1_dbt_team_1_independent():

    @task.datacoves_dbt(connection_id="main_key_pair")
    def build_team_1_independent():
        # Select team_1 models, exclude those downstream of core
        return "dbt build --select path:models/team_1 --exclude path:models/team_1,path:models/core+"

    build_team_1_independent()


L1_dbt_team_1_independent()
```

### Step 4: Team + Core DAG (Dataset-Triggered)

```python
# orchestrate/dags/multi_team/L1_dbt_team_1_with_core.py
"""
## Team 1 Models with Core Dependencies
Builds Team 1 models that depend on Core models.
Triggered automatically when Core DAG completes.
Uses intersection operator to select only core-dependent models.
"""

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils
from orchestrate.utils.datasets import DBT_CORE_COMPLETE


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Team 1",
        owner_email="team1@example.com"
    ),
    description="Build Team 1 models that depend on Core",
    schedule=[DBT_CORE_COMPLETE],  # Triggered when Core dbt completes!
    tags=["L1", "team_1", "dbt", "needs_core"],
)
def L1_dbt_team_1_with_core():

    @task.datacoves_dbt(connection_id="main_key_pair")
    def build_team_1_with_core():
        # Select team_1 models that ARE downstream of core (intersection)
        return "dbt build --select path:models/team_1,path:models/core+"

    build_team_1_with_core()


L1_dbt_team_1_with_core()
```

### Step 5: EL DAGs (Producers for dbt DAGs)

```python
# orchestrate/dags/multi_team/L0_el_core.py
"""
## Core EL Pipeline
Loads data from Core sources and triggers Core dbt DAG.
"""

from airflow.decorators import dag, task, task_group
from orchestrate.utils import datacoves_utils
from orchestrate.utils.datasets import EL_CORE_COMPLETE


@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Data Platform",
        owner_email="data-platform@example.com"
    ),
    description="Load Core data sources",
    schedule=datacoves_utils.set_schedule("0 5 * * *"),  # 5 AM daily
    tags=["L0", "core", "el"],
)
def L0_el_core():

    @task_group(group_id="extract_and_load", tooltip="Load Core Sources")
    def extract_and_load():

        @task.datacoves_bash
        def load_products():
            return "echo 'Loading products data...'"

        @task.datacoves_bash
        def load_orders():
            return "echo 'Loading orders data...'"

        [load_products(), load_orders()]

    @task(outlets=[EL_CORE_COMPLETE])
    def mark_el_complete():
        """Marks EL as complete to trigger downstream dbt DAG"""
        print("Core EL complete - triggering dbt DAG via Dataset")

    extract_and_load() >> mark_el_complete()


L0_el_core()
```

---

## Multiple Dataset Dependencies

If a DAG needs BOTH Core AND another dependency, use a list:

```python
from orchestrate.utils.datasets import DBT_CORE_COMPLETE, REFERENCE_DATA_COMPLETE

@dag(
    # Triggered when BOTH datasets are updated (logical AND)
    schedule=[DBT_CORE_COMPLETE, REFERENCE_DATA_COMPLETE],
    ...
)
def L1_dbt_team_1_with_core_and_reference():
    ...
```

For logical OR (trigger when ANY dataset updates):

```python
@dag(
    # Triggered when EITHER dataset is updated (logical OR)
    schedule=(DBT_CORE_COMPLETE | REFERENCE_DATA_COMPLETE),
    ...
)
def L1_dbt_team_1_any_trigger():
    ...
```

---

## Complete DAG Inventory

| DAG Name | Schedule | dbt Selector | Produces Dataset | Triggered By |
|----------|----------|--------------|------------------|--------------|
| `L0_el_core` | `0 5 * * *` | N/A | `EL_CORE_COMPLETE` | Cron |
| `L0_el_team_1` | `0 5 * * *` | N/A | `EL_TEAM_1_COMPLETE` | Cron |
| `L0_el_team_2` | `0 5 * * *` | N/A | `EL_TEAM_2_COMPLETE` | Cron |
| `L1_dbt_core` | Dataset | `path:models/core` | `DBT_CORE_COMPLETE` | `EL_CORE_COMPLETE` |
| `L1_dbt_team_1_independent` | Dataset | `path:models/team_1 --exclude path:models/team_1,path:models/core+` | - | `EL_TEAM_1_COMPLETE` |
| `L1_dbt_team_2_independent` | Dataset | `path:models/team_2 --exclude path:models/team_2,path:models/core+` | - | `EL_TEAM_2_COMPLETE` |
| `L1_dbt_team_1_with_core` | Dataset | `path:models/team_1,path:models/core+` | - | `DBT_CORE_COMPLETE` |
| `L1_dbt_team_2_with_core` | Dataset | `path:models/team_2,path:models/core+` | - | `DBT_CORE_COMPLETE` |

---

## Pros and Cons

### Hybrid DAG Approach - Pros

- **Maximizes parallelism** - Team-independent models run immediately
- **Event-driven** - No polling with Datasets
- **Failure isolation** - Team failures don't block other teams
- **Modular** - Easy to add new teams
- **Dynamic dependency detection** - Intersection operator auto-detects Core dependencies
- **No special folders** - Standard team folder structure works
- **Native Airflow UI support** - Dataset lineage visible in UI

### Hybrid DAG Approach - Cons

- **More DAGs to maintain** - Mitigate with templating patterns
- **Selector complexity** - Intersection syntax requires understanding
- **Dataset naming convention** - Team must agree on URI scheme

---

## Migration Checklist

- [ ] Organize models into team folders (`models/core/`, `models/team_1/`, etc.)
- [ ] Update `dbt_project.yml` schema mappings if needed
- [ ] Add dataset definitions to `orchestrate/utils/datasets.py`
- [ ] Create EL DAGs with dataset outlets
- [ ] Create Core DAG with dataset outlet
- [ ] Create team-independent DAGs (EL-triggered, with `--exclude` intersection)
- [ ] Create team-with-core DAGs (Core dataset-triggered, with intersection selector)
- [ ] Test selectors locally: `dbt ls --select path:models/team_1,path:models/core+`
- [ ] Test in My Airflow (local) environment
- [ ] Deploy to Team Airflow

---

## Testing Selectors Locally

Before deploying, verify your selectors work correctly:

```bash
# List team_1 models that depend on core (should show core-dependent models)
dbt ls --select path:models/team_1,path:models/core+

# List team_1 models that do NOT depend on core (should show independent models)
dbt ls --select path:models/team_1 --exclude path:models/team_1,path:models/core+

# Verify the union covers all team_1 models
dbt ls --select path:models/team_1
```

---

## References

- [Airflow Datasets Documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- [dbt Node Selection Syntax](https://docs.getdbt.com/reference/node-selection/syntax)
- [dbt Set Operators (intersection)](https://docs.getdbt.com/reference/node-selection/set-operators)
- [dbt Path Selectors](https://docs.getdbt.com/reference/node-selection/methods#path)
- [Datacoves Airflow Decorators](https://docs.datacoves.com)
