"""Broken DAG: config. Task uses a connection id that does not exist."""

from datetime import datetime

from airflow.sdk import Connection, dag, task


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
        conn = Connection.get("salesforce_prd")
        print(f"Connecting to {conn.host}")

    push_to_crm()


broken_bad_connection()
