"""Broken DAG: code bug. Downstream task reads a key the upstream payload never had."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="broken_python_keyerror",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["broken", "code_bug"],
    default_args={"retries": 0},
)
def broken_python_keyerror():
    @task
    def extract_orders() -> dict:
        return {"order_id": 42, "amount": 19.99, "currency": "USD"}

    @task
    def compute_tax(order: dict) -> float:
        # Bug: upstream renamed "country" away; nobody updated this task.
        return order["amount"] * TAX_RATES[order["country"]]

    compute_tax(extract_orders())


TAX_RATES = {"US": 0.07, "DE": 0.19}

broken_python_keyerror()
