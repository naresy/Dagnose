"""Push account updates to the CRM."""

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
    dag_id="demo_crm_push",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crm"],
    default_args={"retries": 0},
)
def demo_crm_push():
    @task
    def push_to_crm() -> None:
        conn = get_connection("salesforce_prd")
        print(f"Connecting to {conn.host}")

    push_to_crm()


demo_crm_push()
