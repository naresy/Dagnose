"""Partner data export."""

import time
from datetime import datetime, timedelta

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(
    dag_id="demo_partner_export",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["partners"],
    default_args={"retries": 0},
)
def demo_partner_export():
    @task(execution_timeout=timedelta(seconds=10))
    def export_partner_data() -> None:
        for page in range(1, 100):
            print(f"Fetching page {page} from partner API ...")
            time.sleep(3)

    export_partner_data()


demo_partner_export()
