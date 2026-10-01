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

Credentials are the same as for the dlt scripts: dlt reads the `datacoves_snowflake` destination from
`~/.dlt/secrets.toml` or, in Airflow, from the variables set by `datacoves_utils.set_dlt_env_vars`.

## Running

`cd` into this folder, then:

```bash
alias cartage='uvx --from "cartage[dlt]>=0.3" --with "dlt[snowflake,duckdb,parquet]" --with pandas cartage'
cartage validate
cartage plan us_population
cartage run us_population                # dev: Snowflake
cartage run usgs_earthquake --env local  # local: balboa.duckdb, no Snowflake needed
cartage state show usgs_earthquake
```
