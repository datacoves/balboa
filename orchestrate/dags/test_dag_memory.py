from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


default_args = {
    "owner": "Alejandro",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

def consume_memory(initial_size_mb, increment_mb, delay_seg, iterations):
    memory_consumed = []
    for i in range(iterations):
        current_size_bytes = (initial_size_mb + i * increment_mb) * 1024 * 1024
        bloque = bytearray(current_size_bytes)
        memory_consumed.append(bloque)
        print(f"Iteration {i+1}: Consuming {len(bloque) / (1024 * 1024):.2f} MB")
        time.sleep(delay_seg)


with DAG(
    dag_id="consume_memory_pod",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["version_6"],
) as dag:
    task_consume = PythonOperator(
        task_id='consume_memory_incremental',
        python_callable=consume_memory,
        op_kwargs={
            'initial_size_mb': 100,
            'increment_mb': 50,
            'delay_seg': 5,
            'iterations': 20
        },
    )
