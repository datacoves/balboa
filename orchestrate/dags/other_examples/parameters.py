"""
## Sample DAG with parameters
This DAG is a sample demonstrating parameter usage with including date picker.
"""

from datetime import datetime, timedelta
from typing import Dict

from airflow.decorators import dag, task
from airflow.models.param import Param

from orchestrate.utils import datacoves_utils


def get_default_dates() -> Dict[str, str]:
    today = datetime.now()
    default_end = today.strftime('%Y-%m-%d')
    default_start = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    return {'start_date': default_start, 'end_date': default_end}

defaults = get_default_dates()

@dag(
    doc_md = __doc__,
    catchup = False,

    default_args = datacoves_utils.set_default_args(
        owner = "Noel Gomez",
        owner_email = "noel@example.com"
    ),

    schedule = datacoves_utils.set_schedule("0 0 1 */12 *"),

    description="Dag with parameters",

    params={
        'process_start_date': Param(
            default=defaults['start_date'],
            type='string',
            format='date',
            description='Start date for data processing'
        ),
        'process_end_date': Param(
            default=defaults['end_date'],
            type='string',
            format='date',
            description='End date for data processing'
        ),
        'processing_mode': Param(
            default='full',
            type='string',
            enum=['full', 'incremental'],
            description='Data processing mode'
        ),
        'batch_size': Param(
            default=1000,
            type='integer',
            minimum=100,
            maximum=5000,
            description='Number of records to process in each batch'
        )
    },

    tags=['sample', 'parameters']
)
def parameterized_example():

    @task()
    def validate_dates(process_start: str, process_end: str) -> dict:  # Changed parameter names here
        """Validate and process date parameters"""
        start = datetime.strptime(process_start, '%Y-%m-%d')
        end = datetime.strptime(process_end, '%Y-%m-%d')

        if end < start:
            raise ValueError("End date must be after start date")

        date_range = (end - start).days

        return {
            "start_date": process_start,
            "end_date": process_end,
            "date_range_days": date_range
        }

    @task()
    def process_data(dates: dict) -> str:
        """Process data using the parameters"""
        print(f"Processing data from {dates['start_date']} to {dates['end_date']}")
        print(f"Total days to process: {dates['date_range_days']}")

        return f"Processed {dates['date_range_days']} days of data"

    @task()
    def final_report(process_result: str) -> None:
        """Generate final report"""
        print(f"Job completed: {process_result}")

    # Execute tasks
    dates = validate_dates(
        process_start="{{ params.process_start_date }}",
        process_end="{{ params.process_end_date }}"
    )

    process_result = process_data(dates)
    final_report(process_result)

parameterized_example()
