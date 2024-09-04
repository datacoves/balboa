#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.python import is_venv_installed

log = logging.getLogger(__name__)

if not is_venv_installed():
    raise AirflowException(
        "The tutorial_taskflow_api_virtualenv example DAG requires virtualenv, please install it."
    )
else:

    @dag(
        schedule=None,
        start_date=datetime(2024, 5, 22),
        catchup=False,
        tags=["example", "version_1"],
    )
    def example_virtualenv():
        """
        ### TaskFlow API example using virtualenv
        This is a simple data pipeline example which demonstrates the use of
        the TaskFlow API virtualenv capabilities
        """

        @task.virtualenv(
            system_site_packages=False,
            requirements=["pyjokes"],
        )
        def tell_a_joke():
            """
            A funny task that tells a joke after installing pyjokes inside a virtualenv
            """
            import json

            import pyjokes

            return pyjokes.get_joke()

        tell_a_joke()

    tutorial_dag = example_virtualenv()
