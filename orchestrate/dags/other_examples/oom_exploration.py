import time

import pendulum
from airflow.decorators import dag, task
from kubernetes.client import models as k8s


def memory_limit(limit: str) -> dict:
    """Pin the worker pod's memory. The container must be named 'base'."""
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"memory": limit},
                            limits={"memory": limit},
                        ),
                    )
                ]
            )
        )
    }


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["oom-test"],
    default_args={"retries": 0},
)
def oom_test():
    @task(executor_config=memory_limit("64Mi"))
    def oom_before_start():
        # Airflow's task runner alone needs more than 64Mi, so the pod
        # should be OOMKilled before this body ever runs.
        print("If you see this, raise the limit is too high to test pre-start OOM")

    @task(executor_config=memory_limit("512Mi"))
    def oom_after_start():
        print("Task started; allocating memory until the pod is OOMKilled")
        hog = []
        for i in range(1, 100):
            # Writing real bytes forces the pages to be committed.
            hog.append(bytearray(b"\x01") * (50 * 1024 * 1024))
            print(f"Allocated ~{i * 50} MiB", flush=True)
            time.sleep(1)

    oom_before_start()
    oom_after_start()


oom_test()
