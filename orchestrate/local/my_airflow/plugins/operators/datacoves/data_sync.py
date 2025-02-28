import json
import os

from operators.datacoves.bash import DatacovesBashOperator

from airflow.hooks.base import BaseHook

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
        connection_id: str = "",
        service_connection_name: str = "",
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
        if service_connection_name and connection_id:
            raise ValueError(
                "Only one of 'service_connection_name' or 'connection_id' should be provided"
            )
        if connection_id:
            self.airflow_connection_name = connection_id
        else:
            self.airflow_connection_name = None
            self.service_connection_name = service_connection_name or "load_airflow"
        self.destination_type = destination_type
        if additional_tables:
            tables += additional_tables
        self.tables = list(set(tables))
        # Construct environment variables needed by `dbt-coves data-sync`
        source_database = destination_schema or self._get_source_database()
        command = f"dbt-coves data-sync {destination_type} --source {source_database}"
        tables_quoted = f'"{",".join(self.tables)}"'
        command += f" --tables {tables_quoted}"
        task_id = (
            f"data_sync_airflow_to_{connection_id}"
            if connection_id
            else f"data_sync_airflow_to_{self.service_connection_name}"
        )
        super().__init__(
            task_id=task_id,
            bash_command=command,
        )

    def execute(self, context):
        self.env = self._get_env_for_data_sync()
        self.append_env = True
        return super().execute(context)

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
        self.fields = [
            "PASSWORD",
            "SCHEMA",
        ]
        self.extra_fields = ["ACCOUNT", "WAREHOUSE", "DATABASE", "ROLE"]
        super().__init__(
            destination_type="snowflake",
            tables=tables,
            additional_tables=additional_tables,
            destination_schema=destination_schema,
            *args,
            **kwargs,
        )

    def _load_env_vars_from_airflow_connection(
        self, env: dict, datasync_prefix: str
    ) -> dict:
        """Load environment variables from Airflow Connection"""
        conn = BaseHook.get_connection(self.airflow_connection_name)
        conn_extra_fields = json.loads(conn.extra)
        for key in self.fields:
            env[f"{datasync_prefix}{key}"] = getattr(conn, key.lower())
        for key in self.extra_fields:
            env[f"{datasync_prefix}{key}"] = conn_extra_fields.get(key.lower(), "")
        env[f"{datasync_prefix}USER"] = getattr(conn, "login")
        return env

    def _get_env_for_data_sync(self) -> dict:
        """Define env variables for dbt-coves data-sync"""
        datasync_prefix = "DATA_SYNC_SNOWFLAKE_"
        env = {
            "DATA_SYNC_SOURCE_CONNECTION_STRING": self._get_airflow_db_conn_string(),
            f"{datasync_prefix}TYPE": "SNOWFLAKE",
        }
        # we can either retrieve them from Airflow Connection or environment variables
        if self.airflow_connection_name:
            env = self._load_env_vars_from_airflow_connection(env, datasync_prefix)
        else:
            datacoves_prefix = f"DATACOVES__{self.service_connection_name.upper()}__"
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
        self.fields = ["HOST", "PASSWORD"]
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
        if self.airflow_connection_name:
            conn = BaseHook.get_connection(self.airflow_connection_name)
            for key in self.fields:
                env[f"{datasync_prefix}{key}"] = getattr(conn, key.lower())
            env[f"{datasync_prefix}DATABASE"] = getattr(conn, "schema")
            env[f"{datasync_prefix}USER"] = getattr(conn, "login")
        else:
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
