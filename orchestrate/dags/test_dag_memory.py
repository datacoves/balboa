from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

def consume_memory(initial_size_mb, increment_mb, delay_seg, iterations):
    memory_consumed = []
    for i in range(iterations):
        current_size_bytes = (initial_size_mb + i * increment_mb) * 1024 * 1024
        bloque = bytearray(current_size_bytes)
        memory_consumed.append(bloque)
        print(f"Iteración {i+1}: Consumiendo {len(bloque) / (1024 * 1024):.2f} MB")
        time.sleep(delay_seg)


with DAG(
    dag_id='consume_memory_pod',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["version_1"],
) as dag:
    task_consume = PythonOperator(
        task_id='consumir_memoria_gradual',
        python_callable=consumir_memoria,
        op_kwargs={
            'initial_size_mb': 100,
            'increment_mb': 200,
            'delay_seg': 5,
            'iterations': 20
        },
    )
