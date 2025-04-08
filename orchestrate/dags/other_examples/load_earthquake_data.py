import datetime
from airflow.decorators import dag, task
from orchestrate.utils import datacoves

@dag(
    default_args={
        "start_date": datetime.datetime(2024, 1, 1, 0, 0),
        "owner": "Noel Gomez",
        "email": "gomezn@example.com",
        "email_on_failure": True,
        "retries": 3
    },
    description="Sample DAG demonstrating bad variable usage",
    schedule="0 0 1 */12 *",
    tags=["extract_and_load","transform"],
    catchup=False,
)
def load_earthquake_data():

    # Find the last time the DAG was successful
    @task
    def get_last_success_date(**context):
        dag_id = context['dag'].dag_id
        print(f"DAG: {dag_id}")
        success_date = datacoves.last_dag_successful_run(dag_id)
        if success_date == None:
            success_date = datetime.date.today()
            print(f"##### NOTE: Last DAG run date set to: {success_date}")
        else:
            success_date = success_date.date()
        return str(success_date - datetime.timedelta(days=3))

    start_date = {"DATACOVES__START_DATE": get_last_success_date()}

    # Load earthquake data from USGS
    @task.datacoves_bash(
        env = {**start_date, **datacoves.connection_to_env_vars("main_load"), **datacoves.uv_env_vars()}
    )
    def load_usgs_data(**context):
        # return "env| sort |grep DATAC"
        return "cd load/dlt && ./usgs_earthquake.py --start-date $DATACOVES__START_DATE"

    # Load Country Polygon Data
    @task.datacoves_bash(
        env = {**datacoves.connection_to_env_vars("main_load"), **datacoves.uv_env_vars()}
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
