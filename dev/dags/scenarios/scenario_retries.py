"""Test scenario: a task that fails on every try and has retries.

Expected: one diagnosis and one card for the final failure, not one per try.
"""

from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="scenario_retries",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dagnose-test"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=5)},
)
def scenario_retries():
    @task
    def call_partner_api() -> None:
        raise ConnectionError("partner-api.example.com:443 refused the connection")

    call_partner_api()


scenario_retries()
