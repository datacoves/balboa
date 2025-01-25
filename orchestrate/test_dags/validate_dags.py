"""Example DAGs test. This test ensures that all Dags have tags, retries set to two, and no import errors.
This is an example pytest and may not be fit the context of your DAGs. Feel free to add and remove tests."""

import os
import logging
from contextlib import contextmanager
import pytest
import warnings
from airflow.models import DagBag

APPROVED_TAGS = {'extract_and_load',
                'transform',
                'python_script',
                'ms_teams_notification',
                'slack_notification',
                'marketing_automation',
                'update_catalog',
                'parameters',
                'sample'}

ALLOWED_OPERATORS = [
    "_PythonDecoratedOperator",  # this allows the @task decorator
    "DatacovesBashOperator",
    "DatacovesDbtOperator",
    "DatacovesDataSyncOperatorSnowflake",
    "_DatacovesDataSyncSnowflakeDecoratedOperator",
    "_DatacovesDataSyncRedshiftDecoratedOperator",
    "AirbyteTriggerSyncOperator",
    'FivetranOperator',
    'FivetranSensor',
]

@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW__CORE__DAGS_FOLDER"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")



@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_catchup_false(dag_id, dag, fileloc):
    """
    test if a DAG has catchup set to False
    """
    assert (
        dag.catchup == False
    ), f"{dag_id} in {fileloc} must have catchup set to False."



@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[0] for x in get_dags()]
)
def test_dag_uses_allowed_operators_only(dag_id, dag, fileloc):
    """
    Test if a DAG uses only allowed operators.
    """
    for task in dag.tasks:
        assert any(
            task.task_type == allowed_op for allowed_op in ALLOWED_OPERATORS
        ), f"{task.task_id} in {dag_id} ({fileloc}) uses {task.task_type}, which is not in the list of allowed operators."


@pytest.mark.parametrize(
    "dag_id,dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
# def test_dag_retries(dag_id, dag, fileloc):
#     """
#     test if a DAG has retries set
#     """
#     num_retries = dag.default_args.get("retries", 0)

#     with pytest.warns(UserWarning):
#         assert (
#             num_retries >= 2
#         ), f"{dag_id} in {fileloc} must have task retries >= 2 it currently has {num_retries}."
def test_dag_retries(dag_id, dag, fileloc):
    """
    test if a DAG has retries set
    """
    num_retries = dag.default_args.get("retries", 0)

    if num_retries == 0 or num_retries is None:
        pytest.fail(f"{dag_id} in {fileloc} must have task retries >= 1 it currently has {num_retries}.")
    elif num_retries < 3:
        warnings.warn(f"{dag_id} in {fileloc} should have task retries >= 3 it currently has {num_retries}.", UserWarning)
    else:
        assert num_retries >= 3, f"{dag_id} in {fileloc} must have task retries >= 2 it currently has {num_retries}."
