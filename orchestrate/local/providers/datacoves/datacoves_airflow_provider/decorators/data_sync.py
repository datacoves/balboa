from __future__ import annotations

from typing import Callable, Sequence

from operators.datacoves.data_sync import (
    DatacovesDataSyncOperatorRedshift,
    DatacovesDataSyncOperatorSnowflake,
)

from airflow.decorators.base import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)


class _DatacovesDataSyncSnowflakeDecoratedOperator(
    DecoratedOperator, DatacovesDataSyncOperatorSnowflake
):
    """
    Wraps a Python callable and uses the callable return value as the Bash command

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked
        in your function (templated).
    :param op_args: A list of positional arguments that will get unpacked when
        calling your callable (templated).
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *DatacovesDataSyncOperatorSnowflake.template_fields,
    )
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **DatacovesDataSyncOperatorSnowflake.template_fields_renderers,
    }

    custom_operator_name: str = "@task.datacoves_data_sync_snowflake"
    service_connection_name = ""


class _DatacovesDataSyncRedshiftDecoratedOperator(
    DecoratedOperator, DatacovesDataSyncOperatorRedshift
):
    """
    Wraps a Python callable and uses the callable return value as the Bash command

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked
        in your function (templated).
    :param op_args: A list of positional arguments that will get unpacked when
        calling your callable (templated).
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *DatacovesDataSyncOperatorRedshift.template_fields,
    )
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **DatacovesDataSyncOperatorRedshift.template_fields_renderers,
    }

    custom_operator_name: str = "@task.datacoves_data_sync_redshift"
    service_connection_name = ""


def datacoves_data_sync_task(
    db_type: str,
    destination_schema: str = "",
    additional_tables: list[str] = [],
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function into a BashOperator.

    Accepts kwargs for operator kwargs. Can be reused in a single DAG. This function is only used only used
    during type checking or auto-completion.

    :param python_callable: Function to decorate.

    :meta private:
    """
    if db_type.lower() not in ["snowflake", "redshift"]:
        raise ValueError(
            "db_type is required and must be either 'snowflake' or 'redshift'"
        )
    else:
        return task_decorator_factory(
            destination_schema=destination_schema,
            additional_tables=additional_tables,
            python_callable=python_callable,
            decorated_operator_class=(
                _DatacovesDataSyncSnowflakeDecoratedOperator
                if db_type.lower() == "snowflake"
                else _DatacovesDataSyncRedshiftDecoratedOperator
            ),
            **kwargs,
        )
