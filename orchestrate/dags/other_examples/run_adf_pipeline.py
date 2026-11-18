"""
## Run an Azure Data Factory pipeline
Starts an ADF pipeline, then waits for it with a deferrable sensor. While the sensor waits, it runs in
the Airflow triggerer instead of holding a worker slot. The sensor fails if the pipeline run fails.

Requires `apache-airflow-providers-microsoft-azure` and an `Azure Data Factory` connection with the
Client ID, Secret, Tenant ID and Subscription ID of a service principal that has the
`Data Factory Contributor` role on the factory's resource group.
Set Resource Group Name and Factory Name in the connection too.
"""

from airflow.sdk import dag, task_group
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor

from orchestrate.utils import datacoves_utils

# The operator and sensor require resource_group_name and factory_name, but None makes the
# hook use the Resource Group Name and Factory Name set in the connection
ADF_ARGS = {
    "azure_data_factory_conn_id": "datacoves_azure_data_factory",
    "resource_group_name": None,
    "factory_name": None,
}


@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    description = "Trigger an Azure Data Factory pipeline and wait for it with a deferrable sensor",
    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),
    tags = ["extract_and_load"],
)
def run_adf_pipeline():

    @task_group
    def adf_pipeline(pipeline_name):
        # Start the pipeline and return immediately; the run_id is pushed to XCom
        run_pipeline = AzureDataFactoryRunPipelineOperator(
            task_id = "run_pipeline",
            pipeline_name = pipeline_name,
            # parameters = {"my_param": "value"},  # only parameters the pipeline declares
            wait_for_termination = False,
            **ADF_ARGS,
        )

        # Wait for the run to finish in the triggerer
        wait_for_pipeline = AzureDataFactoryPipelineRunStatusSensor(
            task_id = "wait_for_pipeline",
            run_id = run_pipeline.output["run_id"],
            deferrable = True,
            poke_interval = 60,         # seconds between status checks
            timeout = 60 * 60 * 2,      # fail if the run takes more than 2 hours
            **ADF_ARGS,
        )

        run_pipeline >> wait_for_pipeline

    adf_pipeline("myTestPipeline")

run_adf_pipeline()
