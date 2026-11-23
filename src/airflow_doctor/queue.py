"""Job hand-off between the Airflow listener (producer) and the worker (consumer).

Two parts:
- Spool: the listener writes one small JSON file per failed try into a spool folder
  (write to a temp file, then atomic rename). No locks, so many Airflow processes can
  write at once, even over Docker bind mounts or NFS where SQLite locking is unsafe.
- JobQueue: a SQLite queue that only the worker process touches. It ingests the spool
  and tracks status, attempts and errors.

Standard library only: this module is imported inside Airflow processes, so it must
be cheap to import and must not pull in the rest of Airflow Doctor.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_id      TEXT    NOT NULL,
    run_id      TEXT    NOT NULL,
    task_id     TEXT    NOT NULL,
    try_number  INTEGER,
    map_index   INTEGER NOT NULL DEFAULT -1,
    failed_at   TEXT,
    status      TEXT    NOT NULL DEFAULT 'pending',  -- pending | running | done | error
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    enqueued_at REAL    NOT NULL,
    updated_at  REAL    NOT NULL,
    not_before  REAL    NOT NULL DEFAULT 0,  -- retry backoff: not claimable before this time
    UNIQUE (dag_id, run_id, task_id, try_number, map_index)
);
"""

# Created after migrations, so they also apply to queues made by older versions.
_INDEXES = """
CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, id);
-- Small key/value store for worker state, e.g. when the API was last polled.
CREATE TABLE IF NOT EXISTS worker_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
-- The UNIQUE above treats NULL try_numbers as all different; this one does not.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_unique_try
    ON jobs (dag_id, run_id, task_id, IFNULL(try_number, -1), map_index);
"""


def default_home() -> Path:
    """AIRFLOW_DOCTOR_HOME, else ~/.airflow-doctor."""
    return Path(os.environ.get("AIRFLOW_DOCTOR_HOME") or Path.home() / ".airflow-doctor")


_JOB_FIELDS = ("dag_id", "run_id", "task_id", "try_number", "map_index", "failed_at")


def write_spool_job(spool_dir: Path, job: dict[str, Any]) -> Path:
    """Atomically drop one job file into the spool. Safe with many concurrent writers."""
    tmp_dir = spool_dir / ".tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    name = f"{time.time_ns()}-{uuid.uuid4().hex}.json"
    tmp = tmp_dir / name
    tmp.write_text(json.dumps({k: job.get(k) for k in _JOB_FIELDS}))
    final = spool_dir / name
    os.replace(tmp, final)  # atomic on the same filesystem; readers never see half a file
    return final


@dataclass(frozen=True)
class QueuedJob:
    id: int
    dag_id: str
    run_id: str
    task_id: str
    try_number: int | None
    map_index: int
    failed_at: str | None
    attempts: int


class JobQueue:
    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path) if path else default_home() / "queue.db"

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None: we manage transactions explicitly with BEGIN IMMEDIATE.
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.executescript(_SCHEMA)
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(jobs)")}
            if "not_before" not in columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN not_before REAL NOT NULL DEFAULT 0")
            conn.executescript(_INDEXES)
            yield conn
        finally:
            conn.close()

    def enqueue(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int | None = None,
        map_index: int = -1,
        failed_at: str | None = None,
    ) -> bool:
        """Add a job. Returns False if this exact task try is already queued."""
        now = time.time()
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO jobs (dag_id, run_id, task_id, try_number, map_index,"
                " failed_at, enqueued_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (dag_id, run_id, task_id, try_number, map_index, failed_at, now, now),
            )
            return cur.rowcount == 1

    def ingest_spool(self, spool_dir: Path) -> int:
        """Move spool files into the queue. Returns the number of new jobs.
        Unreadable files go to spool_dir/bad/ so they are kept but not retried."""
        if not spool_dir.is_dir():
            return 0
        added = 0
        for path in sorted(spool_dir.glob("*.json")):
            try:
                job = json.loads(path.read_text())
                added += self.enqueue(
                    dag_id=str(job["dag_id"]),
                    run_id=str(job["run_id"]),
                    task_id=str(job["task_id"]),
                    try_number=job.get("try_number"),
                    map_index=-1 if job.get("map_index") is None else int(job["map_index"]),
                    failed_at=job.get("failed_at"),
                )
            except (ValueError, KeyError, TypeError):
                bad_dir = spool_dir / "bad"
                bad_dir.mkdir(exist_ok=True)
                os.replace(path, bad_dir / path.name)
                continue
            path.unlink()
        return added

    def claim(self) -> QueuedJob | None:
        """Take the oldest pending job that is due, and mark it running, atomically."""
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM jobs WHERE status = 'pending' AND not_before <= ?"
                    " ORDER BY id LIMIT 1",
                    (time.time(),),
                ).fetchone()
                if row is None:
                    conn.execute("COMMIT")
                    return None
                conn.execute(
                    "UPDATE jobs SET status = 'running', attempts = attempts + 1, updated_at = ?"
                    " WHERE id = ?",
                    (time.time(), row["id"]),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return QueuedJob(
            id=row["id"],
            dag_id=row["dag_id"],
            run_id=row["run_id"],
            task_id=row["task_id"],
            try_number=row["try_number"],
            map_index=row["map_index"],
            failed_at=row["failed_at"],
            attempts=row["attempts"] + 1,
        )

    def mark_done(self, job_id: int) -> None:
        self._set_status(job_id, "done", None)

    def mark_error(self, job_id: int, error: str, *, retry: bool, retry_after_s: float = 0) -> None:
        """retry=True puts the job back as pending, claimable after `retry_after_s`."""
        self._set_status(job_id, "pending" if retry else "error", error)
        if retry and retry_after_s > 0:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE jobs SET not_before = ? WHERE id = ?",
                    (time.time() + retry_after_s, job_id),
                )

    def requeue_stale(self, older_than_s: float) -> int:
        """Return jobs stuck in 'running' (worker died) to 'pending'."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE jobs SET status = 'pending', updated_at = ?"
                " WHERE status = 'running' AND updated_at < ?",
                (time.time(), time.time() - older_than_s),
            )
            return cur.rowcount

    def get_state(self, key: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM worker_state WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row["value"])

    def set_state(self, key: str, value: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO worker_state (key, value) VALUES (?, ?)", (key, value)
            )

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status").fetchall()
        return {status: n for status, n in rows}

    def _set_status(self, job_id: int, status: str, error: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE jobs SET status = ?, last_error = ?, updated_at = ? WHERE id = ?",
                (status, error, time.time(), job_id),
            )
