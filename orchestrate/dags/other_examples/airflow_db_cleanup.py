"""
## Airflow Database Cleanup DAG

This DAG automates the cleanup of Airflow's metadata database using the `airflow db clean` command.
To learn more check out https://www.astronomer.io/docs/learn/2.x/cleanup-dag-tutorial

### Why is this needed?
Airflow retains all historical task data indefinitely. Over time, tables like `dag_run`, `job`,
`log`, `task_instance`, and `xcom` can grow significantly, causing:
- Delayed task scheduling and DAG parsing
- Sluggish UI performance
- Increased storage costs

### How it works
1. **db_cleanup**: Executes `airflow db clean` to remove old records
2. **drop_archive_tables**: Removes temporary archive tables created during cleanup

### Parameters
- **clean_before_timestamp**: Delete records older than this date (default: 180 days ago)
- **tables**: Which metadata tables to clean (default: all eligible tables)
- **dry_run**: Preview SQL commands without executing (default: True for safety)

### Usage
1. Trigger the DAG manually from the Airflow UI
2. Configure parameters as needed
3. For first run, use `dry_run=True` to preview changes
4. Review logs, then run with `dry_run=False` to execute cleanup
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

from orchestrate.utils import datacoves_utils

# Tables eligible for cleanup via `airflow db clean`
CLEANABLE_TABLES = [
    "callback_request",
    "dag",
    "dag_run",
    "dataset_event",
    "import_error",
    "job",
    "log",
    "session",
    "sla_miss",
    "task_fail",
    "task_instance",
    "task_instance_history",
    "task_reschedule",
    "trigger",
    "xcom"
]

def get_default_clean_before_date() -> str:
    """Returns date 180 days ago as default cleanup cutoff."""
    return (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

@dag(
    doc_md=__doc__,
    catchup=False,
    default_args=datacoves_utils.set_default_args(
        owner="Noel Gomez",
        owner_email="noel@example.com"
    ),
    # Manual trigger only - no schedule
    schedule=None,
    description="Clean up Airflow metadata database to improve performance",
    tags=["maintenance", "cleanup", "database"],
    render_template_as_native_obj=True,
    params={
        "clean_before_timestamp": Param(
            default=get_default_clean_before_date(),
            type="string",
            format="date",
            description="Delete records older than this date (YYYY-MM-DD)",
        ),
        "tables": Param(
            default=CLEANABLE_TABLES,
            type="array",
            description="Metadata tables to clean (leave default for all eligible tables)",
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
            description="Preview SQL commands without executing (recommended for first run)",
        ),
    },
    
    description="Clean up Airflow metadata database to improve performance",
    tags=["maintenance"],
)
def airflow_db_cleanup():

    @task()
    def db_cleanup(
        clean_before_timestamp: str,
        tables: list,
        dry_run: bool,
    ) -> dict:
        """
        Execute the airflow db clean command.

        This removes old records from metadata tables while preserving
        the most recent data needed for operations.
        """
        import subprocess

        # Build the command
        cmd = [
            "airflow",
            "db",
            "clean",
            "--clean-before-timestamp",
            clean_before_timestamp,
            "--skip-archive",
            "--yes",
        ]

        # Add tables to clean
        for table in tables:
            cmd.extend(["--tables", table])

        # Add dry run flag if enabled
        if dry_run:
            cmd.append("--dry-run")

        print(f"Executing command: {' '.join(cmd)}")

        # Execute the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise Exception(
                f"airflow db clean failed with return code {result.returncode}"
            )

        return {
            "status": "success",
            "dry_run": dry_run,
            "clean_before": clean_before_timestamp,
            "tables_cleaned": tables,
        }

    @task(trigger_rule="all_done")
    def drop_archive_tables(cleanup_result: dict) -> str:
        """
        Drop any archive tables created during cleanup.

        Archive tables are temporary tables that may be left behind if
        the cleanup process times out or fails. This task ensures they
        are removed.

        Uses trigger_rule='all_done' to run even if db_cleanup fails.
        """
        import subprocess

        cmd = [
            "airflow",
            "db",
            "drop-archived",
            "--yes",
        ]

        print(f"Executing command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )

        print(result.stdout)
        if result.stderr:
            print(result.stderr)

        if result.returncode != 0:
            raise Exception(
                f"airflow db drop-archived failed with return code {result.returncode}"
            )

        return "Archive tables dropped successfully"

    # Execute tasks
    cleanup_result = db_cleanup(
        clean_before_timestamp="{{ params.clean_before_timestamp }}",
        tables="{{ params.tables }}",
        dry_run="{{ params.dry_run }}",
    )

    drop_archive_tables(cleanup_result)


airflow_db_cleanup()
