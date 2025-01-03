# conftest.py
import os

import pytest
from datacoves_airflow_provider.testing.custom_reporter import *


@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    os.environ["AIRFLOW_VAR_MY_VAR1"] = "my_var1"
    os.environ["AIRFLOW_VAR_MY_VAR2"] = "my_var2"
    os.environ["AIRFLOW_VAR_MY_VAR3"] = "my_var3"
