"""Daily revenue report."""

import sqlite3
from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="demo_daily_revenue",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["finance", "reporting"],
    default_args={"retries": 0},
)
def demo_daily_revenue():
    @task
    def daily_revenue() -> int:
        conn = sqlite3.connect("/tmp/warehouse.db")
        conn.execute("CREATE TABLE IF NOT EXISTS customers (id INTEGER, name TEXT)")
        row = conn.execute(
            "SELECT SUM(amount) FROM orders WHERE order_date = date('now')"
        ).fetchone()
        return row[0] or 0

    daily_revenue()


demo_daily_revenue()
