from __future__ import annotations

import warnings
from typing import Any, Callable, Collection, Mapping, Sequence

from operators.datacoves.dbt import DatacovesDbtOperator

from airflow.decorators.base import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.types import NOTSET


class _DatacovesDbtDecoratedOperator(DecoratedOperator, DatacovesDbtOperator):
    """
    Wraps a Python callable and uses the callable return value as the Bash command to be executed.

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked
        in your function (templated).
    :param op_args: A list of positional arguments that will get unpacked when
        calling your callable (templated).
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *DatacovesDbtOperator.template_fields,
    )
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **DatacovesDbtOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.datacoves_dbt"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )

        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            bash_command=NOTSET,
            multiple_outputs=False,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        bash_command = self.python_callable(*self.op_args, **kwargs)
        self.bash_command = self._get_full_command(bash_command)
        if not isinstance(self.bash_command, str) or self.bash_command.strip() == "":
            raise TypeError(
                "The returned value from the TaskFlow callable must be a non-empty string."
            )
        return super().execute(context)


def datacoves_dbt_task(
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
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_DatacovesDbtDecoratedOperator,
        **kwargs,
    )
