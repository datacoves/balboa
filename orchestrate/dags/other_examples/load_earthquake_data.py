"""
## Sample DAG Loading and transforming earthquake data
This DAG shows how do use dlt and dbt to replicate [End-to-end Fabric Pipeline](https://www.youtube.com/watch?v=Av44Nrhl05s)
"""
import datetime

from airflow.decorators import dag, task
from orchestrate.utils import datacoves_utils

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args=datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    description="DAG using dlt and dbt",
    tags=["extract_and_load","transform"],
)
def load_earthquake_data():

    # Find the last time the DAG was successfully
    @task
    def get_last_success_date(**context):
        dag_id = context['dag'].dag_id
        print(f"DAG: {dag_id}")
        success_date = datacoves_utils.last_dag_successful_run(dag_id)
        if success_date == None:
            success_date = datetime.date.today()
            print(f"##### NOTE: Last DAG run date set to: {success_date}")
        else:
            success_date = success_date.date()
        return str(success_date - datetime.timedelta(days=3))

    start_date = {"DATACOVES__START_DATE": get_last_success_date()}


    # Load earthquake data from USGS
    @task.datacoves_bash(
        env = {**start_date, **datacoves_utils.connection_to_env_vars("main_load"), **datacoves_utils.uv_env_vars()}
    )
    def load_usgs_data(**context):
        # return "env| sort |grep DATAC"
        return "cd load/dlt && ./usgs_earthquake.py --start-date $DATACOVES__START_DATE"


    # Load Country Polygon Data
    @task.datacoves_bash(
        env = {**datacoves_utils.connection_to_env_vars("main_load"), **datacoves_utils.uv_env_vars()}
    )
    def load_country_geography():
        return "cd load/dlt && ./country_geo.py"


    # Run the dbt transformations
    @task.datacoves_dbt(
        connection_id="main",
    )
    def transform():
        return "dbt build -s tag:earthquake_analysis+"


    # Define Dependencies
    [load_usgs_data(), load_country_geography()] >> transform()

# Invoke DAG
load_earthquake_data()
