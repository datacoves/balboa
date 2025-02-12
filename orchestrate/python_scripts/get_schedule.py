import os
from typing import Union

DEV_ENVIRONMENT_SLUG = "dev123" # Replace with your environment slug

def get_schedule(default_input: Union[str, None]) -> Union[str, None]:
    """
    Sets the application's schedule based on the current environment setting. Allows you to
    set the the default for dev to none and the the default for prod to the default input.

    This function checks the Datacoves Slug through 'DATACOVES__ENVIRONMENT_SLUG' variable to determine
    if the application is running in a specific environment (e.g., 'dev123'). If the application
    is running in the 'dev123' environment, it indicates that no schedule should be used, and
    hence returns None. For all other environments, the function returns the given 'default_input'
    as the schedule.

   Parameters:
    - default_input (Union[str, None]): The default schedule to return if the application is not
      running in the dev environment.

    Returns:
    - Union[str, None]: The default schedule if the environment is not 'dev123'; otherwise, None,
      indicating that no schedule should be used in the dev environment.
    """
    env_slug = os.environ.get("DATACOVES__ENVIRONMENT_SLUG", "").lower()
    if env_slug == DEV_ENVIRONMENT_SLUG:
        return None
    else:
        return default_input
