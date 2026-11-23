"""Order tax calculation."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="demo_orders_tax",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finance"],
    default_args={"retries": 0},
)
def demo_orders_tax():
    @task
    def extract_orders() -> dict:
        return {"order_id": 42, "amount": 19.99, "currency": "USD"}

    @task
    def compute_tax(order: dict) -> float:
        rate = TAX_RATES[order["country"]]
        return order["amount"] * rate

    compute_tax(extract_orders())


TAX_RATES = {"US": 0.07, "DE": 0.19}

demo_orders_tax()
