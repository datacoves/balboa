def get_provider_info():
    return {
        "package-name": "datacoves-airflow-provider",
        "name": "Datacoves Airflow Provider",
        "description": "An Airflow provider for Datacoves",
        "versions": ["0.0.2"],
        "hook-class-names": [],
        "extra-links": [],
        "operators": [],
        "sensors": [],
        "secrets-backends": [],
        "transfers": [],
        "hooks": [],
        "executors": [],
        "task-decorators": [
            {
                "name": "datacoves_bash",
                "class-name": "datacoves_airflow_provider.decorators.bash.datacoves_bash_task",
            },
            {
                "name": "datacoves_dbt",
                "class-name": "datacoves_airflow_provider.decorators.dbt.datacoves_dbt_task",
            },
            {
                "name": "datacoves_airflow_db_sync",
                "class-name": "datacoves_airflow_provider.decorators.data_sync.datacoves_data_sync_task",
            },
        ],
    }
