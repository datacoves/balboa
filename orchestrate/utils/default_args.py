# utils/default_args.py
from datetime import datetime, timedelta

default_args = {
    'owner': 'mayra@datacoves.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 12, 1),
}
