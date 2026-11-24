"""Test scenario: a mapped (dynamic) task where one of three instances fails.

Expected: the failure of map index 1 is found and diagnosed on its own, with the
right map index in the card and the link.
"""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="scenario_mapped",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dagnose-test"],
    default_args={"retries": 0},
)
def scenario_mapped():
    @task
    def load_region(region: str) -> int:
        rows = {"emea": 120, "apac": None, "amer": 340}[region]
        return rows + 0  # APAC has no row count today

    load_region.expand(region=["emea", "apac", "amer"])


scenario_mapped()
