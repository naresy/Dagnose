"""Test scenario: a task that writes a very long log before failing.

Expected: Dagnose keeps the last lines (DAGNOSE_LOG_TAIL_LINES, 500 by default),
still finds the exception, and the diagnosis stays fast and cheap.
"""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="scenario_big_log",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dagnose-test"],
    default_args={"retries": 0},
)
def scenario_big_log():
    @task
    def reconcile_ledger() -> None:
        for i in range(20_000):
            print(f"reconciled entry {i}: ok")
        raise ValueError("ledger out of balance by 12.50 EUR after 20000 entries")

    reconcile_ledger()


scenario_big_log()
