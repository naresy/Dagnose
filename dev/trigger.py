"""Trigger DAG runs through the Airflow REST API (Airflow 2 or 3), for testing.

Uses the same DAGNOSE_AIRFLOW_* settings as Dagnose, but the user needs permission to
trigger runs (Viewer cannot; use an Op or Admin user for testing).

    uv run python dev/trigger.py scenario_retries
    uv run python dev/trigger.py demo_crm_push --times 10      # burst / repeat test
    uv run python dev/trigger.py demo_orders_tax demo_crm_push  # several DAGs
"""

from __future__ import annotations

import argparse
import sys

from dagnose.collector import AirflowAPIError, AirflowClient
from dagnose.collector.airflow_api import _seg
from dagnose.config import Settings


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("dag_ids", nargs="+")
    parser.add_argument("--times", type=int, default=1, help="runs per DAG")
    args = parser.parse_args()

    settings = Settings()
    failed = 0
    with AirflowClient(settings) as client:
        body: dict[str, object] = {"logical_date": None} if client.api == "v2" else {}
        for dag_id in args.dag_ids:
            try:  # DAGs are often paused at creation
                client._request("PATCH", f"/dags/{_seg(dag_id)}", json={"is_paused": False})
            except AirflowAPIError as exc:
                print(f"{dag_id}: could not unpause ({exc})")
            for _ in range(args.times):
                try:
                    run = client._request("POST", f"/dags/{_seg(dag_id)}/dagRuns", json=body).json()
                    print(f"{dag_id}: triggered {run['dag_run_id']}")
                except AirflowAPIError as exc:
                    print(f"{dag_id}: FAILED to trigger: {exc}")
                    failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
