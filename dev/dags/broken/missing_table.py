"""Broken DAG: upstream data. Query against a table that was never created."""

import sqlite3
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="broken_missing_table",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["broken", "upstream_data"],
    default_args={"retries": 0},
)
def broken_missing_table():
    @task
    def daily_revenue() -> int:
        conn = sqlite3.connect("/tmp/warehouse.db")
        conn.execute("CREATE TABLE IF NOT EXISTS customers (id INTEGER, name TEXT)")
        # The orders table is loaded by another pipeline that has not run.
        row = conn.execute(
            "SELECT SUM(amount) FROM orders WHERE order_date = date('now')"
        ).fetchone()
        return row[0] or 0

    daily_revenue()


broken_missing_table()
