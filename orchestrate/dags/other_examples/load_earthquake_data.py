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
        success_date = datacoves_utils.get_last_dag_successful_run_date(dag_id)
        if success_date == None:
            success_date = datetime.date.today()
            print(f"##### NOTE: Last DAG run date set to: {success_date}")
        else:
            success_date = success_date.date()
        return str(success_date - datetime.timedelta(days=3))

    # Load earthquake data from USGS
    @task.datacoves_bash
    def load_usgs_data(**context):
        from orchestrate.utils import datacoves_utils

        # Get the start date directly from the upstream task
        task_instance = context['task_instance']
        start_date = task_instance.xcom_pull(task_ids = 'get_last_success_date')

        # Set up environment variables
        env_vars = datacoves_utils.set_dlt_env_vars({'destinations': ['main_load_keypair']})
        env_vars['DATACOVES__START_DATE'] = start_date

        env_exports = datacoves_utils.generate_env_exports(env_vars)

        return f"{env_exports}; cd load/dlt && ./usgs_earthquake.py --start-date $DATACOVES__START_DATE"

    # Load Country Polygon Data
    @task.datacoves_bash
    def load_country_geography():
        from orchestrate.utils import datacoves_utils

        env_vars = datacoves_utils.set_dlt_env_vars({'destinations': ['main_load_keypair']})

        env_exports = datacoves_utils.generate_env_exports(env_vars)

        return f"{env_exports}; cd load/dlt && ./country_geo.py"

    # Run the dbt transformations
    @task.datacoves_dbt(
        connection_id="main_key_pair",
    )
    def transform():
        return "dbt build -s tag:earthquake_analysis+"

    # First, save the task instance of get_last_success_date
    get_start_date = get_last_success_date()
    get_usgs_data = load_usgs_data()
    get_geography_data = load_country_geography()
    trasnform_data =  transform()

    # Set dependencies
    get_start_date >> get_usgs_data
    [get_usgs_data, get_geography_data] >> trasnform_data

load_earthquake_data()
