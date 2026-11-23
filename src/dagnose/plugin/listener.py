"""Airflow listener: drop failed task tries into the Dagnose spool folder.

Runs inside Airflow processes, so it must be fast, must never raise, and imports only
the standard-library queue module. All real work happens in `dagnose worker`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from airflow.listeners import hookimpl

from dagnose.queue import default_home, write_spool_job

log = logging.getLogger(__name__)


# The hook spec differs between Airflow versions (2.x passes `session`, 2.10+ and 3.x pass
# `error`). Pluggy only passes the arguments an implementation names, so naming just these
# two works on all of them.
@hookimpl
def on_task_instance_failed(previous_state: Any, task_instance: Any) -> None:
    try:
        map_index = getattr(task_instance, "map_index", -1)
        write_spool_job(
            default_home() / "spool",
            {
                "dag_id": task_instance.dag_id,
                "run_id": task_instance.run_id,
                "task_id": task_instance.task_id,
                "try_number": getattr(task_instance, "try_number", None),
                "map_index": -1 if map_index is None else int(map_index),
                "failed_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    except Exception:  # never let Dagnose break Airflow
        log.warning("dagnose: could not enqueue failed task", exc_info=True)
