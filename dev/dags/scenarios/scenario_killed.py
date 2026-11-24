"""Test scenario: the task process is killed from outside, as the kernel does when a
task runs out of memory.

It simulates the kill with SIGKILL on its own process instead of really exhausting
memory, so it is safe to run on a shared Airflow. Expected: the failure is found by
API polling (the task's own listener cannot run), and the card says what is known.
"""

import os
import signal
from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="scenario_killed",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dagnose-test"],
    default_args={"retries": 0},
)
def scenario_killed():
    @task
    def build_features() -> None:
        print("loading 40M rows into memory ...")
        os.kill(os.getpid(), signal.SIGKILL)

    build_features()


scenario_killed()
