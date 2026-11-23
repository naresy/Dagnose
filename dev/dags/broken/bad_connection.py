"""Broken DAG: config. Task uses a connection id that does not exist."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

try:
    from airflow.sdk import Connection

    get_connection = Connection.get
except ImportError:  # Airflow 2.x
    from airflow.hooks.base import BaseHook

    get_connection = BaseHook.get_connection


@dag(
    dag_id="broken_bad_connection",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["broken", "config"],
    default_args={"retries": 0},
)
def broken_bad_connection():
    @task
    def push_to_crm() -> None:
        # Typo: the real connection is "salesforce_prod".
        conn = get_connection("salesforce_prd")
        print(f"Connecting to {conn.host}")

    push_to_crm()


broken_bad_connection()
