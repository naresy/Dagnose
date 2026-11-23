"""Sync CRM sessions into the warehouse."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

WAREHOUSE_DSN = "postgresql://etl_user:Sup3r-S3cret-Pw@warehouse.internal:5432/sales"
CRM_SESSION = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJldGwtYm90In0.ZmFrZS1zaWduYXR1cmUtZm9yLXRlc3Rz"
ONCALL_EMAIL = "data-oncall@example.com"


@dag(
    dag_id="demo_crm_session_sync",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["crm"],
    default_args={"retries": 0},
)
def demo_crm_session_sync():
    @task
    def sync_crm(crm_api_key: str, region: str) -> None:
        print(f"Connecting to {WAREHOUSE_DSN}")
        print(f"POST https://crm.example.com/api/sync?region={region}&access_token={crm_api_key}")
        print(f"Paging {ONCALL_EMAIL}")
        raise PermissionError(f"CRM rejected session {CRM_SESSION} for {WAREHOUSE_DSN}")

    sync_crm(crm_api_key="ck_7Hq2LmX9pR4vT8wZ3nB6yK1d", region="eu-west-1")


demo_crm_session_sync()
