import os

from operators.datacoves.bash import DatacovesBashOperator

DEFAULT_AIRFLOW_TABLES = [
    "ab_permission",
    "ab_role",
    "ab_user",
    "dag",
    "dag_run",
    "dag_tag",
    "import_error",
    "job",
    "task_fail",
    "task_instance",
]


class DatacovesDataSyncOperator(DatacovesBashOperator):
    """
    Extract data from a source and load into a destination by calling `dbt-coves data-sync`
    """

    template_fields = ("service_connection_name", "tables")

    def __init__(
        self,
        destination_type: str,
        tables: list = DEFAULT_AIRFLOW_TABLES,
        additional_tables: list = [],
        destination_schema: str = "",
        service_connection_name: str = "load_airflow",
        *args,
        **kwargs,
    ) -> None:
        """
        destination_type: indicates destination: i.e. snowflake.
        service_connection_name: defined in the Datacoves environment.
                                 Destination of the data sync.
        """
        if not destination_type:
            raise ValueError(
                "DatacovesDataSyncOperator is not meant to be used directly",
                "Use DatacovesDataSyncOperatorSnowflake or DatacovesDataSyncOperatorRedshift variants instead",
            )
        self.service_connection_name = service_connection_name
        if additional_tables:
            tables += additional_tables
        self.tables = list(set(tables))
        # Construct environment variables needed by `dbt-coves data-sync`
        source_database = destination_schema or self._get_source_database()
        command = f"dbt-coves data-sync {destination_type} --source {source_database}"
        tables_quoted = f'"{",".join(self.tables)}"'
        command += f" --tables {tables_quoted}"
        super().__init__(
            task_id=f"data_sync_airflow_to_{self.service_connection_name}",
            bash_command=command,
            env=self._get_env_for_data_sync(),
            append_env=True,
        )

    def _get_env_for_data_sync(self) -> dict:
        raise NotImplementedError

    def _get_airflow_db_conn_string(self) -> str:
        return os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")

    def _get_source_database(self) -> str:
        full_url = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")
        # https://airflow-<env slug>.<domain> => split to get 'airflow-<env slug>'
        value = full_url.split("://")[1].split(".")[0]
        return value


class DatacovesDataSyncOperatorSnowflake(DatacovesDataSyncOperator):
    def __init__(
        self,
        tables: list = DEFAULT_AIRFLOW_TABLES,
        additional_tables: list = [],
        destination_schema="",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            destination_type="snowflake",
            tables=tables,
            additional_tables=additional_tables,
            destination_schema=destination_schema,
            *args,
            **kwargs,
        )

    def _get_env_for_data_sync(self) -> dict:
        """Define env variables for dbt-coves data-sync"""
        env = {"DATA_SYNC_SOURCE_CONNECTION_STRING": self._get_airflow_db_conn_string()}
        datacoves_prefix = f"DATACOVES__{self.service_connection_name.upper()}__"
        datasync_prefix = "DATA_SYNC_SNOWFLAKE_"
        for key in [
            "ACCOUNT",
            "DATABASE",
            "PASSWORD",
            "ROLE",
            "SCHEMA",
            "USER",
            "WAREHOUSE",
        ]:
            env[f"{datasync_prefix}{key}"] = os.environ.get(
                f"{datacoves_prefix}{key}", ""
            )

        # TODO this type isn't being used anywhere
        env[f"{datasync_prefix}TYPE"] = "SNOWFLAKE"
        return env


class DatacovesDataSyncOperatorRedshift(DatacovesDataSyncOperator):
    def __init__(
        self,
        tables: list = DEFAULT_AIRFLOW_TABLES,
        additional_tables: list = [],
        destination_schema="",
        *args,
        **kwargs,
    ) -> None:
        super().__init__(
            destination_type="redshift",
            tables=tables,
            additional_tables=additional_tables,
            destination_schema=destination_schema,
            *args,
            **kwargs,
        )

    def _get_env_for_data_sync(self) -> dict:
        """Define env variables for dbt-coves data-sync"""
        env = {"DATA_SYNC_SOURCE_CONNECTION_STRING": self._get_airflow_db_conn_string()}
        datacoves_prefix = f"DATACOVES__{self.service_connection_name.upper()}__"
        datasync_prefix = "DATA_SYNC_REDSHIFT_"
        for key in [
            "DATABASE",
            "PASSWORD",
            "SCHEMA",
            "USER",
            "HOST",
        ]:
            env[f"{datasync_prefix}{key}"] = os.environ.get(
                f"{datacoves_prefix}{key}", ""
            )
        return env
