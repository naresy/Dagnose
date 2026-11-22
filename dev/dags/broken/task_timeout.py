"""Broken DAG: timeout. Task runs longer than its execution_timeout."""

import time
from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="broken_task_timeout",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["broken", "timeout"],
    default_args={"retries": 0},
)
def broken_task_timeout():
    @task(execution_timeout=timedelta(seconds=10))
    def slow_api_export() -> None:
        for page in range(1, 100):
            print(f"Fetching page {page} from partner API ...")
            time.sleep(3)

    slow_api_export()


broken_task_timeout()
