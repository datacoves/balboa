# Cartage examples

The [load/dlt](../dlt) examples rebuilt with [Cartage](https://github.com/datacoves/cartage), plus a transformation
example. Each pipeline is a short YAML file: the source, optional Python transforms, and one or more destinations.
Cartage runs them on dlt, keeps incremental state, and generates the Airflow DAGs.

## Quick start

`cd` into this folder. Nothing to install: `uvx` fetches Cartage and the drivers.

```bash
alias cartage='uvx --from "cartage[dlt]>=0.4.3" --with "dlt[snowflake,duckdb,parquet]" --with pandas cartage'

cartage validate                                  # check every pipeline, connection and transform
cartage plan us_population_documents -n 2         # preview the transformation, writes nothing
cartage run us_population_documents               # writes output/json/states.json and output/xml/states.xml
cartage run us_population --env local             # load into balboa.duckdb instead of Snowflake
cartage run us_population                         # dev: Snowflake, with your ~/.dlt/secrets.toml
```

The first four commands need no credentials.

## Pipelines

| Pipeline                  | Replaces                 | What it shows                                                        |
| ------------------------- | ------------------------ | -------------------------------------------------------------------- |
| `us_population_documents` | (new)                    | CSV → Python transforms → JSON and XML files                         |
| `us_population`           | `us_population.py`       | a CSV over HTTP → `us_population`                                    |
| `loans_data`              | `loans_data.py`          | 2 CSVs → `loans`; PII tags and change tracking after the load        |
| `country_populations`     | `country_populations.py` | a CSV into the `RAW` database (`snowflake_raw` connection)           |
| `country_geo`             | `country_geo.py`         | GeoJSON flattened to one row per country                             |
| `usgs_earthquake`         | `usgs_earthquake.py`     | an API, merged on `id`; continues from the last event it loaded      |

The datasets and tables are the same as the dlt scripts'. What moved out of the scripts:

- **Destination, dataset, write disposition, keys, column types** → the pipeline YAML.
- **Credentials** → `connections.yaml`, per environment (see below).
- **Post-load SQL** (`apply_pii_tag`, `enable_change_tracking`) → `after_load` hooks; the same functions, in
  `utils/datacoves_utils.py`.
- **The earthquake `--start-date`** → Cartage state: each run starts 3 days before the last event loaded
  (`incremental: { cursor: properties.time, lag: ... }`), so the DAG doesn't compute it.

## Transformation example

The US population CSV is wide, with one column per year and numbers as text:

```text
states,id,2010,2011,...,2019
Alabama,1,"4,785,437","4,799,069",...,"4,903,185"
```

`pipelines/us_population_documents.yaml` reshapes it into one document per state with two Python steps from
`transforms/population.py`, then writes the result twice:

| Step / destination | Does                                                                          |
| ------------------ | ----------------------------------------------------------------------------- |
| `map: by_year`     | year columns → `populations: [{year, population}]` as integers, plus `growth_pct` |
| `filter: at_least` | keeps states with at least 1,000,000 people (`with: { population: 1000000 }`) |
| `json_files`       | `output/json/states.json`                                                     |
| `xml_files`        | `output/xml/states.xml`                                                       |

`cartage plan` shows each source row next to what it becomes, or why it was dropped (once per destination):

```text
───────── record 1 ─────────
source:
  states: Alabama
  '2010': 4,785,437
  ...
transformed:
  state: Alabama
  populations:
  - year: 2010
    population: 4785437
  ...
  growth_pct: 2.46
───────── record 2 ─────────
source:
  states: Alaska
  ...
transformed: none (filtered out)
```

```xml
<states>
  <record>
    <state>Alabama</state>
    <populations>
      <item><year>2010</year><population>4785437</population></item>
      ...
    </populations>
    <growth_pct>2.46</growth_pct>
  </record>
```

Both outputs are small dlt destinations defined in this project (`sinks/documents.py`) and referenced from
`connections.yaml` as `destination: sinks.documents:json_file`. Each pipeline destination runs separately, with
its own state, so one can fail and be retried without rewriting the other.

## Environments and credentials

No credentials are stored here.

| Env     | Snowflake connections load into   | Credentials from                                                         |
| ------- | --------------------------------- | ------------------------------------------------------------------------ |
| `local` | `balboa.duckdb` (DuckDB file)     | none needed                                                              |
| `dev`   | Snowflake (default env)           | `~/.dlt/secrets.toml`, `destination.datacoves_snowflake` (see `../dlt/.dlt`) |
| `prd`   | Snowflake, run by Airflow         | the `main_load_keypair` Airflow connection, via `${airflow:...}` references |

The `prd` references only resolve inside the generated DAGs; locally, `cartage` stops with a hint. The JSON and XML
connections are the same in every environment. The PII-tag and change-tracking hooks only run on Snowflake.

## Layout

| Path                         | Contains                                                                    |
| ---------------------------- | --------------------------------------------------------------------------- |
| `cartage.yaml`               | environments, engine, state location, Airflow settings                      |
| `connections.yaml`           | Snowflake, DuckDB, JSON and XML destinations per environment                |
| `pipelines/*.yaml`           | one file per pipeline                                                       |
| `sources/`                   | dlt sources: CSV/GeoJSON over HTTP (`web.py`), USGS API (`usgs.py`)         |
| `transforms/population.py`   | the `map` and `filter` steps of the transformation example                  |
| `sinks/documents.py`         | the JSON and XML destinations                                               |
| `utils/datacoves_utils.py`   | Snowflake `after_load` hooks                                                |
| `templates/airflow/dag.py.j2`| makes generated DAGs use `datacoves_utils.set_default_args`                 |
| `.cartage/`, `output/`       | local state and output files (git-ignored)                                  |

## Airflow

`cartage generate` writes one DAG per scheduled pipeline to `orchestrate/dags/cartage/`. Each DAG runs
`cartage run <pipeline> --env prd` with `DatacovesBashOperator`, passes the Airflow connection fields the pipeline
uses, and adds the uv/dlt worker settings from `cartage.yaml` (`task_env`). Like the other balboa DAGs, they use
`datacoves_utils.set_default_args` and `set_schedule` (no schedule in My Airflow), via `templates/airflow/dag.py.j2`. Regenerate after changing a pipeline,
`cartage.yaml` or `connections.yaml`; `cartage generate --check` fails if the DAGs are stale. The transformation
example writes local files, so it has no schedule.

## Adding a pipeline

1. Write the source in `sources/`: a function returning a dlt resource or source.
2. Add `pipelines/<name>.yaml` with `source.ref: sources.<module>:<function>`, a destination connection and, if needed,
   `transforms`.
3. `cartage validate`, `cartage plan <name>`, then `cartage run <name> --env local`.
4. To schedule it, add `schedule: { airflow: { schedule: "..." } }` and run `cartage generate`.
