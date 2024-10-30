"""
This dag runs end to end data procesing for BODB schema.

1. Run ADF pipeline extract_redshift with execution key JNJ_SNAP.
2. Run ADF pipeline extract_sharepoint_list with execution key BODB_ONE_MSL.
3. Run snowflake execution framework with execution key JNJ_SNAP and BODB_ONE_MSL.
4. Wait for the completion of the PPCA snowflake execution framework. A table from the PPCA schema is used in the BODB schema.
3. Run dbt with selector BODB.

"""

import os

# test 7 sync
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import DAG, Variable
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from operators.datacoves.bash import DatacovesBashOperator
from operators.datacoves.dbt import DatacovesDbtOperator

SWITCH_TO_NEW_VENV = """pip freeze > /tmp/reqs.txt && \
    deactivate && \
    python -m venv /tmp/venv && \
    source /tmp/venv/bin/activate && \
    pip install -r /tmp/reqs.txt &&"""

# Get the password from an Airflow variable
password = Variable.get("password")

# Define the shared DAG variables
default_args = {
    "max_active_runs": 1,
    "retries": 0,
    "azure_data_factory_conn_id": "azure_data_factory",
}


def send_email_on_completion(context):

    email = EmailOperator(
        task_id="send_email_on_completion",
        to=context["params"]["notification_emails"],
        subject=f"Airflow {context['reason'].replace('_', ' ')}: {context['task_instance'].dag_id}",
        html_content=f"""
            The execution of the pipeline '{context['task_instance'].dag_id}' with has completed
            with state {context['reason'].replace('_', ' ')}.
            <br><br>
            The configurations for this pipeline execution were:
            <br> {context["params"]}
            <br><br>
            For further details review airflow logs at the following URL:
            <br>  
            {context['task_instance'].log_url}
        """,
    )
    email.execute(context)


def log_dag_run(context):
    bash = DatacovesBashOperator(
        task_id="log_dag_run_on_completion",
        bash_command=f""" {SWITCH_TO_NEW_VENV} \
            cd $DATACOVES__DBT_HOME && \
            pip install wheel && \
            pip install ../orchestrate/custom_python_libs/snowflake_execution_framework/dist/snowflake_execution_framework-0.1.4-py3-none-any.whl  && \
            python ../orchestrate/python_scripts/log_dag_completion.py \
            --dag_id '{context['task_instance'].dag_id}' \
            --reason '{context['reason']}' \
            --log_url '{context['task_instance'].log_url}' \
        """,
    )
    bash.execute(context)


def on_completion(context):
    send_email_on_completion(context)
    log_dag_run(context)


# Change to generic pipeline name.
with DAG(
    dag_id="bruno_datacoves_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 4 * * *",  # every weekday at 4am
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_completion,
    on_success_callback=on_completion,
    params={"notification_emails": Param(["bantone1@kenvue.com"])},
    tags=["scheduled", "daily", "version_3"],
    description="Runs daily BODB data processing end to end",
) as dag:

    # Revise the parameters to enable the execution of various schemas.
    def execute_adf_pipeline(pipeline_name, execution_key):
        run_pipeline = AzureDataFactoryRunPipelineOperator(
            task_id=f"execute_adf_pipeline_{pipeline_name}_execution_key_{execution_key}",
            pipeline_name=pipeline_name,
            parameters={"EXECUTION_KEY": execution_key},
            azure_data_factory_conn_id="azure_data_factory",
            dag=dag,
            factory_name="",
            resource_group_name="",
        )
        return run_pipeline

    # Revise the execution_key to enable the execution of various schemas.
    def execute_snowflake_framework(execution_key):
        bash = DatacovesBashOperator(
            task_id=f"execute_snowflake_sql_execution_key_{execution_key}",
            bash_command=f"{SWITCH_TO_NEW_VENV} \
                cd $DATACOVES__DBT_HOME && \
                pip install wheel && \
                pip install ../orchestrate/custom_python_libs/snowflake_execution_framework/dist/snowflake_execution_framework-0.1.4-py3-none-any.whl  && \
                python ../orchestrate/python_scripts/run_sql_execution_framework.py --execution_key {execution_key} \
            ",
            retries=0,
            dag=dag,
        )
        return bash

    def wait_for_another_dag():
        wait_for = ExternalTaskSensor(
            task_id="wait_for_ppca_snowflake",
            external_dag_id="daily_ppca",
            external_task_id="execute_snowflake_sql_execution_key_PPCA",
            check_existence=True,  # fail fast if dependency does not exist
            poke_interval=60 * 20,  # 20 minutes
            mode="reschedule",  # use reschedule if wait on task is in ten of minutes.
            timeout=60 * 60 * 24,  # 4 hours max wait
            dag=dag,
        )
        return wait_for

    def run_dbt():
        run_dbt = DatacovesDbtOperator(
            task_id="run_dbt_selector_BODB",
            bash_command=f"dbt  run --selector BODB_DAILY_RUN",
            dag=dag,
        )
        return run_dbt

    # Running the Bash command to trigger the tableau report refresh.
    def run_tableau_report_refresh():
        run_tableau_refresh = DatacovesBashOperator(
            task_id="run_tableau_report_refresh",
            bash_command=f"{SWITCH_TO_NEW_VENV} \
                cd $DATACOVES__DBT_HOME && \
                pip install tableauserverclient && \
                pip install wheel && \
                pip install ../orchestrate/custom_python_libs/snowflake_execution_framework/dist/snowflake_execution_framework-0.1.4-py3-none-any.whl  && \
                python ../orchestrate/python_scripts/tableau_refresh_workbook.py \
                --username {Variable.get('username')} \
                --server_ip {Variable.get('server_ip')} \
                --workbook_name 'Business Outlook' \
                --project_name 'EMEA Business Outlook' \
                --site_id 'EMEA' \
            ",
            env={"PASSWORD": password},
            append_env=True,
            dag=dag,
        )
        return run_tableau_refresh

    # https://docs.astronomer.io/learn/managing-dependencies
    chain(
        # [
        #     execute_adf_pipeline("extract_redshift", "JNJ_SNAP"),
        #     execute_adf_pipeline("extract_sharepoint_list", "BODB_ONE_MSL"),
        # ],
        [
            execute_snowflake_framework("JNJ_SNAP"),
            execute_snowflake_framework("BODB_ONE_MSL"),
        ],
        wait_for_another_dag(),
        run_dbt(),
        run_tableau_report_refresh(),
    )
