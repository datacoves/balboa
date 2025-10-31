from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": "DL-JRDUS-RDI-WORKFLOW-NOTIFICATION@ITS.JNJ.com",
    "retries": 0,
}

@dag(

    dag_id = "gra_dh_insight_product_cmc_im360_data_package",
    description = 'To load the L3 models for product cmc data package daily',
    tags = ['gra', 'im360', 'product_cmc'],
    start_date=datetime(2024, 7, 7),
    doc_md = __doc__,
    catchup = False,

    default_args = default_args,
    schedule = '45 6 * * 1-5'
)
def dbt_pipeline():

    # No pod created - runs in scheduler
    start_task = EmptyOperator(task_id='start_task')

    # @task
    # def run_dbt(dbt_command):
    #     # dbt command you want to run, e.g.:
    #     # # "dbt run --full-refresh --select models/l3/rdh_publish/im360_registered_datapackage/*"
    #     venv_activate = "source /opt/datacoves/virtualenvs/main/bin/activate"
    #     full_cmd = f"bash -lc '{venv_activate} && {dbt_command}'"
    #     print(f"Running: {full_cmd}")
    #     subprocess.run(dbt_command, shell=True, check=True, executable="/bin/bash")

    @task.datacoves_dbt(
        connection_id="main"
    )
    def run_dbt(dbt_command):
        return dbt_command

    # No pod created - runs in scheduler
    end_task = EmptyOperator(task_id='end_task')

    # dbt commands
    dbt_l3_im360_registered_datapackage_cmd = "dbt run --full-refresh --select models/l3/rdh_publish/im360_registered_datapackage/*"
    dbt_l3_registration_cmd = "dbt build -s registration__l3__rdh_publish"
    dbt_l3_im360_incremental_models_cmd = "dbt run --full-refresh --select models/l3/rdh_publish/im360_incremental_datapackage/*"
    dbt_l3_im360_non_incremental_models_cmd = "dbt build -s models/l3/rdh_publish/im360_non_incremental_datapackages/* --exclude registered_product_shelf_life__l3__rdh_publish"
    dbt_l3_im360_product_shelf_life_cmd = "dbt build -s registered_product_shelf_life__l3__rdh_publish"

    # Specify task_id when calling the function
    dbt_l3_im360_registered_datapackage = run_dbt.override(task_id='dbt_l3_im360_registered_datapackage')(dbt_l3_im360_registered_datapackage_cmd)
    dbt_l3_registration = run_dbt.override(task_id='dbt_l3_registration')(dbt_l3_registration_cmd)
    dbt_l3_im360_incremental_models = run_dbt.override(task_id='dbt_l3_im360_incremental_models')(dbt_l3_im360_incremental_models_cmd)
    dbt_l3_im360_non_incremental_models = run_dbt.override(task_id='dbt_l3_im360_non_incremental_models')(dbt_l3_im360_non_incremental_models_cmd)
    dbt_l3_im360_product_shelf_life = run_dbt.override(task_id='dbt_l3_im360_product_shelf_life')(dbt_l3_im360_product_shelf_life_cmd)

    start_task >> dbt_l3_registration >> dbt_l3_im360_registered_datapackage >> end_task
    start_task >> dbt_l3_im360_incremental_models >> dbt_l3_im360_product_shelf_life >> end_task
    start_task >> dbt_l3_im360_non_incremental_models >> end_task

dbt_pipeline()
