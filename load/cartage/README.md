# Cartage examples

The [load/dlt](../dlt) examples rebuilt with [Cartage](https://github.com/datacoves/cartage), plus a transformation
example. Each pipeline is a short YAML file: the source, optional Python transforms, and one or more destinations.
Cartage runs them on dlt, keeps incremental state, and generates the Airflow DAGs.

## Quick start

`cd` into this folder. Nothing to install: `uvx` fetches Cartage and the drivers.

```bash
alias cartage='uvx --from "cartage>=0.8.0" --with "dlt[snowflake,duckdb,parquet,http]" cartage'

cartage validate                                  # check every pipeline, connection and transform
cartage plan us_population_documents -n 2         # preview the transformation, writes nothing
cartage run us_population_documents               # writes output/states.json and output/states.xml
cartage run us_population --env dev_duckdb        # load into balboa.duckdb instead of Snowflake
cartage run us_population                         # dev_snowflake: Snowflake, with your ~/.dlt/secrets.toml
```

The first four commands need no credentials.

## Pipelines

| Pipeline                  | Replaces                 | What it shows                                                        |
| ------------------------- | ------------------------ | -------------------------------------------------------------------- |
| `us_population_documents` | (new)                    | CSV → Python transforms → JSON and XML files                         |
| `us_population`           | `us_population.py`       | a CSV over HTTP → `us_population`                                    |
| `personal_loans`          | `loans_data.py`          | a CSV → `loans`; PII tags and change tracking after the load         |
| `zip_coordinates`         | `loans_data.py`          | a CSV → `loans`; change tracking after the load                      |
| `country_populations`     | `country_populations.py` | a CSV into the `RAW` database (`snowflake_raw` connection)           |
| `country_geo`             | `country_geo.py`         | GeoJSON flattened to one row per country                             |
| `usgs_earthquake`         | `usgs_earthquake.py`     | an API, merged on `id`; continues from the last event it loaded      |

The datasets and tables are the same as the dlt scripts'. What moved out of the scripts:

- **Reading files and APIs** → configuration only: CSVs over HTTPS through `filesystem` connections (`github`,
  `datacoves_samples`), the GeoJSON with dlt's `dlt.sources.rest_api:rest_api_source`. Only the USGS source
  is Python (`sources/usgs.py`): it requests one day at a time and turns the cursor into the API's date format.
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
| `exports` (json)   | `output/states.json`                                                          |
| `exports` (xml)    | `output/states.xml`, `<states><record>...</record></states>`                  |

`cartage plan` shows each source row next to what it becomes, or why it was dropped, and the exact JSON or XML it
will write (once per destination):

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

Both outputs use Cartage's built-in `file` destination: one `exports` connection (a folder), and per destination a
`format` (`json`, `jsonl`, `xml` or `csv`) and file name; `name: as_xml` tells the two apart. Files are written to
`*.partial` and moved into place only when the run succeeds. Each destination runs separately, with its own state, so
one can fail and be retried without rewriting the other.

## Environments and credentials

No credentials are stored here.

| Env             | Snowflake connections load into   | Credentials from                                                         |
| --------------- | --------------------------------- | ------------------------------------------------------------------------ |
| `dev_duckdb`    | `balboa.duckdb` (DuckDB file)     | none needed                                                              |
| `dev_snowflake` | Snowflake (default env)           | `~/.dlt/secrets.toml`, `destination.datacoves_snowflake` (see `../dlt/.dlt`) |
| `airflow`       | Snowflake, run by Airflow         | the `main_load_keypair` Airflow connection, via `${airflow:...}` references |

`airflow` is the same in every Airflow: the DAGs read `main_load_keypair` from the Airflow they run in, so a sandbox
Airflow pointing at a dev Snowflake tests exactly the DAGs that are later promoted to production.

The `airflow` references only resolve inside the generated DAGs; locally, `cartage` stops with a hint. The JSON and XML
connections are the same in every environment. The PII-tag and change-tracking hooks only run on Snowflake.

## Layout

| Path                         | Contains                                                                    |
| ---------------------------- | --------------------------------------------------------------------------- |
| `cartage.yaml`               | environments, engine, state location, Airflow settings                      |
| `connections.yaml`           | Snowflake (DuckDB in `dev_duckdb`), the HTTPS file locations, `exports`     |
| `pipelines/*.yaml`           | one file per pipeline                                                       |
| `sources/usgs.py`            | the USGS API source (day-by-day requests, incremental on the event time)    |
| `transforms/population.py`   | the `map` and `filter` steps of the transformation example                  |
| `transforms/geo.py`          | flattens GeoJSON features into one row per country                          |
| `utils/datacoves_utils.py`   | Snowflake `after_load` hooks                                                |
| `.cartage/`, `output/`       | local state and output files (git-ignored)                                  |

## Airflow

`cartage generate` writes one DAG per scheduled pipeline to `orchestrate/dags/cartage/`. Each DAG runs
`cartage run <pipeline> --env airflow` with `@task.datacoves_bash`, passes the Airflow connection fields the pipeline
uses, and adds the uv/dlt worker settings from `cartage.yaml` (`task_env`). Packages come from `dependencies`:
`dlt[snowflake,parquet]` for every DAG in `cartage.yaml`, plus `dlt[http]` in the pipelines that read CSVs over
HTTPS. Like the other balboa DAGs, they use `datacoves_utils.set_default_args` and `set_schedule` (no
schedule in My Airflow), via `default_args_from` and `schedule_from` in `cartage.yaml`. Regenerate after changing a pipeline,
`cartage.yaml` or `connections.yaml`; `cartage generate --check` fails if the DAGs are stale. The transformation
example writes local files, so it has no schedule.

## Adding a pipeline

1. Add `pipelines/<name>.yaml` with a source, a destination connection and, if needed, `transforms`. Files and REST
   APIs need no Python: copy `us_population.yaml` (a file from a `filesystem` connection) or `country_geo.yaml`
   (`dlt.sources.rest_api:rest_api_source`). Connection types and options: Cartage's `docs/connections.md`.
2. Only when configuration can't express the logic, write a source in `sources/` (a function returning a dlt resource
   or source, like `usgs.py`) and reference it as `source.ref: sources.<module>:<function>`.
3. `cartage validate`, `cartage plan <name>`, then `cartage run <name> --env dev_duckdb`.
4. To schedule it, add `schedule: { airflow: { schedule: "..." } }` and run `cartage generate`.
