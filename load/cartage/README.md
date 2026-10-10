# Cartage examples

The [load/dlt](../dlt) examples rebuilt with [Cartage](https://github.com/datacoves/cartage), plus a transformation
example. Each pipeline is a short YAML file: the source, optional Python transforms, and one or more destinations.
Cartage runs them on dlt, keeps incremental state, and generates the Airflow DAGs.

## Create demo files
cd $DATACOVES__REPO_PATH/load/cartage
cartage init demo --answers https://raw.githubusercontent.com/datacoves/cartage/main/examples/sap/answers.yaml --yes


## Quick start

`cd` into this folder. Nothing to install: `uvx` fetches Cartage, and Cartage adds each pipeline's `dependencies`.

```bash
alias cartage='uvx --from "cartage>=0.15.0" cartage'   # pipelines add their own packages (dependencies:)

cartage validate                                  # check every pipeline, connection and transform
cartage plan us_population_documents -n 2         # preview the transformation, writes nothing
cartage run us_population_documents               # writes ~/.cartage/balboa_load/output/states.json and .xml
cartage run us_population --env dev_duckdb        # load into ~/.cartage/balboa_load/balboa.duckdb
cartage run us_population                         # dev_snowflake: Snowflake, with your ~/.dlt/secrets.toml
```

The first four commands need no credentials.

For tab completion (`cartage run us_pop<TAB>`), install Cartage as a command instead of the alias, then turn it on
once and open a new shell:

```bash
uv tool install "cartage>=0.15.0"     # later: uv tool upgrade cartage
cartage --install-completion
```

## Pipelines

| Pipeline                  | Replaces                 | What it shows                                                        |
| ------------------------- | ------------------------ | -------------------------------------------------------------------- |
| `us_population_documents` | (new)                    | CSV → Python transforms → JSON and XML files                         |
| `us_population`           | `us_population.py`       | a CSV over HTTP → `us_population`                                    |
| `personal_loans`          | `loans_data.py`          | a CSV → `loans`; PII tags and change tracking after the load         |
| `zip_coordinates`         | `loans_data.py`          | a CSV → `loans`; change tracking after the load                      |
| `country_populations`     | `country_populations.py` | a CSV → `raw`                                                        |
| `country_geo`             | `country_geo.py`         | GeoJSON flattened to one row per country                             |
| `usgs_earthquake`         | `usgs_earthquake.py`     | an API, merged on `id`; continues from the last event it loaded      |

The datasets and tables are the same as the dlt scripts'. What moved out of the scripts:

- **Reading files and APIs** → configuration only: CSVs over HTTPS through `filesystem` connections (`github`,
  `datacoves_samples`), the GeoJSON with dlt's `dlt.sources.rest_api:rest_api_source`. Only the USGS source
  is Python (`sources/usgs.py`): it requests one day at a time and turns the cursor into the API's date format.
- **Destination, dataset, write disposition, keys, column types** → the pipeline YAML.
- **Credentials** → `.cartage/connections.yaml`, per environment (see below).
- **Post-load SQL** (`apply_pii_tag`, `enable_change_tracking`) → `after_load` hooks; the same functions, in
  `utils/datacoves_utils.py`.
- **The earthquake `--start-date`** → dlt state, kept in Snowflake: each run starts 3 days before the last event loaded
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
| `exports` (json)   | `~/.cartage/balboa_load/output/states.json`                                   |
| `exports` (xml)    | `~/.cartage/balboa_load/output/states.xml`, `<states><record>...</record></states>` |

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

Every pipeline loads into one connection, `warehouse`. It is named for its role, not its type, because the type
changes per environment:

| Env             | `warehouse` is                         | Credentials from                                                         |
| --------------- | -------------------------------------- | ------------------------------------------------------------------------ |
| `dev_duckdb`    | `~/.cartage/balboa_load/balboa.duckdb` | none needed                                                              |
| `dev_snowflake` | Snowflake, `RAW` database (default env) | `~/.dlt/secrets.toml`, `destination.datacoves_snowflake` (see `../dlt/.dlt`) |
| `airflow`       | Snowflake, run by Airflow              | the `main_load_keypair` Airflow connection, via `${airflow:...}` references |

In Snowflake each pipeline's `dataset_name` is the schema, inside the `RAW` database the dbt sources read.
`dev_snowflake` sets `database: raw` itself and takes the rest of the credentials from `~/.dlt/secrets.toml`;
`airflow` takes the database from the Airflow connection.

`airflow` is the same in every Airflow: the DAGs read `main_load_keypair` from the Airflow they run in, so a sandbox
Airflow pointing at a dev Snowflake tests exactly the DAGs that are later promoted to production.

The `airflow` references only resolve inside the generated DAGs; locally, `cartage` stops with a hint. The JSON and XML
connections are the same in every environment. The PII-tag and change-tracking hooks only run on Snowflake.

The pipelines write the same Snowflake tables as the `load/dlt` scripts. `loans_data.py` loads DataFrames, which
leaves out dlt's `_dlt_load_id` column, so the first Cartage run into `loans.personal_loans` or
`loans.zip_coordinates` fails until the table is recreated: run it once with `--full-refresh` (this drops and reloads
the table). After that, load those tables with Cartage only; the dlt script's next load would leave the column empty.

## Layout

| Path                         | Contains                                                                    |
| ---------------------------- | --------------------------------------------------------------------------- |
| `.cartage/config.yaml`       | environments, engine, Airflow settings                                      |
| `.cartage/connections.yaml`  | `warehouse` (DuckDB or Snowflake), the HTTPS file locations, `exports`      |
| `pipelines/*.yaml`           | one file per pipeline                                                       |
| `sources/usgs.py`            | the USGS API source (day-by-day requests, incremental on the event time)    |
| `transforms/population.py`   | the `map` and `filter` steps of the transformation example                  |
| `transforms/geo.py`          | flattens GeoJSON features into one row per country                          |
| `utils/datacoves_utils.py`   | Snowflake `after_load` hooks                                                |
| `answers.yaml`               | starts a new project shaped like this one (see below)                       |
| `~/.cartage/balboa_load/`    | outside the repo: DuckDB file, exports, file export state, rejects (`artifacts_dir`) |

## Airflow

`cartage generate` writes one DAG per scheduled pipeline to `orchestrate/dags/cartage/`. Each DAG runs
`cartage run <pipeline> --env airflow` with `@task.datacoves_bash`, passes the Airflow connection fields the pipeline
uses, and adds the uv/dlt worker settings from `.cartage/config.yaml` (`task_env`). Packages are not installed on the
workers: `cartage run` adds `defaults.dependencies` (`dlt[snowflake,duckdb,parquet]`) and the pipeline's own
`dependencies` (`dlt[http]` for CSVs over HTTPS) with uv, the same as on a laptop. Like the other balboa DAGs, they use `datacoves_utils.set_default_args` and `set_schedule` (no
schedule in My Airflow), via `default_args_from` and `schedule_from` in `.cartage/config.yaml`. Regenerate after changing a pipeline or a file
in `.cartage/`; `cartage generate --check` fails if the DAGs are stale. The transformation
example writes local files, so it has no schedule.

## Adding a pipeline

1. Add `pipelines/<name>.yaml` with a source, a destination connection and, if needed, `transforms`. Files and REST
   APIs need no Python: copy `us_population.yaml` (a file from a `filesystem` connection) or `country_geo.yaml`
   (`dlt.sources.rest_api:rest_api_source`). Connection types and options: Cartage's
   [docs/connections.md](https://github.com/datacoves/cartage/blob/main/docs/connections.md).
2. List the packages it needs beyond `defaults.dependencies` in `.cartage/config.yaml`, e.g.
   `dependencies: ["dlt[http]"]` for files over HTTPS. `cartage run` adds them with uv, here and in Airflow.
3. For reshaping, `cartage scaffold transform <name>` writes `transforms/<name>.py` with `map`, `filter` and `batch`
   examples. Only when configuration can't express the source, `cartage scaffold source <name>` writes
   `sources/<name>.py` (a dlt resource, like `usgs.py`) to reference as `source.ref: sources.<name>:<name>`.
4. `cartage validate`, `cartage plan <name>`, then `cartage run <name> --env dev_duckdb`.
5. To schedule it, add `schedule: { airflow: { schedule: "..." } }` and run `cartage generate`. To change what the
   DAGs look like beyond the settings in `.cartage/config.yaml`, `cartage scaffold airflow` writes a template override.

## Starting your own project

`answers.yaml` answers `cartage init`'s questions: a new project with one pipeline, the US population CSV into a
`warehouse` connection, the same three environments and an Airflow schedule. Run it from any folder:

```bash
cartage init my_load --answers https://raw.githubusercontent.com/datacoves/balboa/main/load/cartage/answers.yaml --yes
cd my_load
cartage run us_population          # dev_duckdb: a copy of the CSV into my_load.duckdb, no credentials
```

Then fill in every `"<fill me>"` that `cartage validate --env dev_snowflake` lists (Snowflake settings in
`.cartage/connections.yaml`, the password in `.cartage/secrets.yaml`, or in `~/.cartage/secrets.yaml` to share it
between projects). Copy the file and change the answers to start from something else; leave any answer out and
`cartage init my_load --answers answers.yaml` asks it instead.
