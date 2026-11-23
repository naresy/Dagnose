"""Broken DAG: dependency. Task imports a package not installed on the worker."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="broken_missing_dependency",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["broken", "dependency"],
    default_args={"retries": 0},
)
def broken_missing_dependency():
    @task
    def score_customers() -> None:
        import polars_ml_toolkit  # not in the worker image

        polars_ml_toolkit.score("customers.parquet")

    score_customers()


broken_missing_dependency()
