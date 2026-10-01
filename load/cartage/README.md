# Cartage examples

The [load/dlt](../dlt) examples as a [Cartage](https://github.com/datacoves/cartage) project: the same sources and
Snowflake datasets, with the pipeline settings in YAML instead of in each script.

| dlt script               | Cartage pipeline                      | Notes                                                    |
| ------------------------ | ------------------------------------- | -------------------------------------------------------- |
| `us_population.py`       | `pipelines/us_population.yaml`        | CSV → `us_population`                                    |
| `loans_data.py`          | `pipelines/loans_data.yaml`           | 2 CSVs → `loans`; PII tags and change tracking via `after_load` |
| `country_populations.py` | `pipelines/country_populations.yaml`  | CSV → `raw.raw` (`snowflake_raw` connection)             |
| `country_geo.py`         | `pipelines/country_geo.yaml`          | GeoJSON → `country_geo`                                  |
| `usgs_earthquake.py`     | `pipelines/usgs_earthquake.yaml`      | merge on `id`; the start date comes from Cartage state (3-day lag), not `--start-date` |

Credentials: `dev` uses the `datacoves_snowflake` destination from `~/.dlt/secrets.toml`, like the dlt scripts.
`prd` reads the `main_load_keypair` Airflow connection through `${airflow:...}` references; it only resolves inside
the generated DAGs.

## Airflow

`cartage generate` writes one DAG per pipeline to `orchestrate/dags/cartage/`. Each DAG runs
`cartage run <pipeline> --env prd` with `DatacovesBashOperator`, passes the Airflow connection fields the pipeline
uses, and uses `datacoves_utils.set_default_args` (see `templates/airflow/dag.py.j2`). Regenerate after changing a
pipeline, `cartage.yaml` or `connections.yaml`; `cartage generate --check` fails if the DAGs are stale.

## Running

`cd` into this folder, then:

```bash
alias cartage='uvx --from "cartage[dlt]>=0.4" --with "dlt[snowflake,duckdb,parquet]" --with pandas cartage'
cartage validate
cartage plan us_population
cartage run us_population                # dev: Snowflake
cartage run usgs_earthquake --env local  # local: balboa.duckdb, no Snowflake needed
cartage state show usgs_earthquake
```
