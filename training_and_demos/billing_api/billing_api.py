import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_URL = os.getenv("DATACOVES__API_URL")
API_KEY = os.getenv("DATACOVES__API_TOKEN")
ENV_SLUG = os.getenv("DATACOVES__ENVIRONMENT")

def get_worker_usage(start_date=None, end_date=None, service=None, env_slug=None):
    """
    Retrieve worker usage minutes for an environment or all environments.

    Args:
        env_slug (str, optional): Environment slug. If None, returns data for all environments
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        service (str, optional): Filter by service type (airflow, airbyte, unknown)

    Returns:
        list: Usage records
    """
    if env_slug:
        url = f"{API_URL}/{env_slug}/"
    else:
        url = f"{API_URL}/"

    params = {}
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    if service:
        params['service'] = service

    response = requests.get(
        url=url,
        headers={"Authorization": f"Token {API_KEY}"},
        params=params
    )

    print(f"Fetching: {response.url}")

    if not response.ok:
        print(response.text)
        return

    response.raise_for_status()
    return response.json()

def calculate_total_minutes_by_service(usage_data):
    """
    Calculate total minutes used per service.

    Args:
        usage_data (list): Usage records from API

    Returns:
        dict: Total minutes per service
    """
    totals = {}

    for record in usage_data:
        service = record['service']
        minutes = record['amount_minutes']
        totals[service] = totals.get(service, 0) + minutes
    return totals

def print_usage_info(usage):

    totals = calculate_total_minutes_by_service(usage)

    for service, minutes in totals.items():
        hours = minutes / 60
        print(f"  {service}: {minutes} minutes ({hours:.2f} hours)")


# Example: Get usage for last 30 days
print("#### Usage for last 30 days ####")
usage = get_worker_usage()
if usage:
    print_usage_info(usage)
else:
    print("No Usage Data Returned for the last 30 days")

# Example: Specific date range
start_date = "2025-07-01"
end_date = "2025-12-27"
print(f"\n#### Usage for {start_date} through {end_date} ####")

usage = get_worker_usage(start_date, end_date)

if usage:
    print_usage_info(usage)
else:
    print(f"No Usage Data Returned for {start_date} to {end_date}")

# Example: specific environment
print(f"\n#### Usage for environment {ENV_SLUG} ####")

usage = get_worker_usage(env_slug=ENV_SLUG)

if usage:
    print_usage_info(usage)
else:
    print(f"No Usage Data Returned for environment {ENV_SLUG} from {start_date} to {end_date}")
