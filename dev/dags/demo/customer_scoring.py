"""Customer scoring."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="demo_customer_scoring",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml"],
    default_args={"retries": 0},
)
def demo_customer_scoring():
    @task
    def score_customers() -> None:
        import polars_ml_toolkit

        polars_ml_toolkit.score("customers.parquet")

    score_customers()


demo_customer_scoring()
