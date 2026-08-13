"""
## OOM Test DAG
Deliberately allocates ~4GB of memory in a single task, then holds it for a
while before releasing. Used to test that OOM detection/alerting pops up a
message when a task/pod runs out of memory.

Trigger manually - not scheduled.

Note: whether this actually gets OOM-killed depends on the memory
limit configured for the worker pod. If the pod's limit is below the
`target_gb` below, the task will be killed while allocating. If the limit
is higher, the task will succeed after holding the memory for `hold_seconds`.
"""
from pendulum import datetime

from airflow.decorators import dag, task

GB = 1024 ** 3

default_args = {
    "start_date": datetime(2024, 1, 1),
    "owner": "Fernando Mercado",
    "email_on_failure": False,
    "retries": 0,
}


@dag(
    doc_md=__doc__,
    catchup=False,
    schedule=None,
    default_args=default_args,
    tags=["test", "oom", "fernando"],
    dag_id="test_oom_fernando",
)
def test_oom_fernando():

    @task
    def consume_memory(target_gb: int = 4, hold_seconds: int = 120):
        import time

        chunk_size = 256 * 1024 * 1024  # 256MB per chunk
        target_bytes = target_gb * GB

        chunks = []
        allocated = 0

        while allocated < target_bytes:
            # bytearray() zero-fills on creation, which forces the pages
            # to become resident rather than just reserved
            chunks.append(bytearray(chunk_size))
            allocated += chunk_size
            print(f"Allocated {allocated / GB:.2f} GB")

        print(f"Holding {allocated / GB:.2f} GB for {hold_seconds}s")
        time.sleep(hold_seconds)
        print("Releasing memory")

    consume_memory()


test_oom_fernando()
