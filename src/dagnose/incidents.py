"""Fingerprints and incidents: the same failure seen again.

A fingerprint identifies "the same failure": DAG, task, exception type, the failing
line in DAG code, and the error message with the parts that change between runs taken
out (numbers, ids, timestamps, secrets' placeholder numbers). Runs that fail the same
way share a fingerprint; a different error, line or task does not.

The incident store (in the worker's SQLite file) counts repeats, remembers the last
diagnosis to reuse, when the last card went out, and how many model calls were made in
the last hour. This is what keeps a DAG that fails every 5 minutes from producing 288
diagnoses and 288 cards a day, and it is the base for incident memory.
"""

from __future__ import annotations

import hashlib
import re
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from dagnose.models import RedactedContext

_SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    fingerprint      TEXT PRIMARY KEY,
    dag_id           TEXT NOT NULL,
    task_id          TEXT NOT NULL,
    signature        TEXT NOT NULL,   -- readable form of what was hashed
    first_seen       REAL NOT NULL,
    last_seen        REAL NOT NULL,
    count            INTEGER NOT NULL,
    diagnosis_path   TEXT,            -- latest diagnosis to reuse
    diagnosed_at     REAL,
    last_notified_at REAL,
    suppressed       INTEGER NOT NULL DEFAULT 0  -- repeats since the last card
);
CREATE TABLE IF NOT EXISTS model_calls (at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS model_calls_at ON model_calls (at);
"""

_VARIABLE = [
    (re.compile(r"\[REDACTED:([a-z_]+)#\d+\]"), r"[REDACTED:\1]"),
    (
        re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I),
        "<uuid>",
    ),
    (re.compile(r"\b0x[0-9a-f]+\b", re.I), "<hex>"),
    (re.compile(r"\b[0-9a-f]{12,}\b", re.I), "<hex>"),
    (
        re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?([+-]\d{2}:?\d{2}|Z)?"),
        "<time>",
    ),
    (re.compile(r"\d+(\.\d+)?"), "<n>"),
]


def _normalize(text: str) -> str:
    for pattern, repl in _VARIABLE:
        text = pattern.sub(repl, text)
    return " ".join(text.split())[:300]


def signature(ctx: RedactedContext) -> str:
    """What makes two failures 'the same', in readable form."""
    job = ctx.job
    exc = ctx.exception
    if exc is not None:
        frame = next((f for f in reversed(exc.frames) if f.in_user_code), None)
        where = f"{frame.filename.rsplit('/', 1)[-1]}:{frame.lineno}" if frame else "-"
        what = f"{exc.type}: {_normalize(exc.message)}"
    else:  # no exception: the last error line of the log, or why the log is missing
        errors = [line.message for line in ctx.log_tail if line.level in ("error", "critical")]
        problems = [e for e in ctx.collection_errors if e.startswith("task log:")]
        where = "-"
        what = _normalize((errors or problems or ["no exception"])[-1])
    return f"{job.dag_id} | {job.task_id} | {where} | {what}"


def fingerprint(ctx: RedactedContext) -> str:
    return hashlib.sha256(signature(ctx).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class Incident:
    fingerprint: str
    count: int  # including this failure
    first_seen: float
    diagnosis_path: str | None  # reusable diagnosis, if still inside the window
    last_notified_at: float | None
    suppressed: int  # repeats not announced since the last card
    is_new: bool


class IncidentStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.executescript(_SCHEMA)
            yield conn
        finally:
            conn.close()

    def record(
        self, ctx: RedactedContext, *, window_s: float, now: float | None = None
    ) -> Incident:
        """Count this failure. A fingerprint not seen within the window starts over."""
        now = time.time() if now is None else now
        fp = fingerprint(ctx)
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM incidents WHERE fingerprint = ?", (fp,)).fetchone()
            if row is None or now - row["last_seen"] > window_s:
                conn.execute(
                    "INSERT OR REPLACE INTO incidents (fingerprint, dag_id, task_id, signature,"
                    " first_seen, last_seen, count, suppressed) VALUES (?, ?, ?, ?, ?, ?, 1, 0)",
                    (fp, ctx.job.dag_id, ctx.job.task_id, signature(ctx), now, now),
                )
                return Incident(fp, 1, now, None, None, 0, is_new=True)
            conn.execute(
                "UPDATE incidents SET last_seen = ?, count = count + 1 WHERE fingerprint = ?",
                (now, fp),
            )
            reusable = row["diagnosis_path"]
            if reusable and not Path(reusable).exists():
                reusable = None
            return Incident(
                fp, row["count"] + 1, row["first_seen"], reusable,
                row["last_notified_at"], row["suppressed"], is_new=False,
            )  # fmt: skip

    def save_diagnosis(self, fp: str, path: Path, now: float | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET diagnosis_path = ?, diagnosed_at = ? WHERE fingerprint = ?",
                (str(path), time.time() if now is None else now, fp),
            )

    def mark_notified(self, fp: str, now: float | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET last_notified_at = ?, suppressed = 0 WHERE fingerprint = ?",
                (time.time() if now is None else now, fp),
            )

    def mark_suppressed(self, fp: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET suppressed = suppressed + 1 WHERE fingerprint = ?", (fp,)
            )

    def take_model_call(self, limit_per_hour: int, now: float | None = None) -> bool:
        """Reserve one model call if the hourly limit allows it (0 = no limit)."""
        now = time.time() if now is None else now
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM model_calls WHERE at < ?", (now - 3600,))
            used = conn.execute("SELECT COUNT(*) FROM model_calls").fetchone()[0]
            if limit_per_hour and used >= limit_per_hour:
                conn.execute("COMMIT")
                return False
            conn.execute("INSERT INTO model_calls (at) VALUES (?)", (now,))
            conn.execute("COMMIT")
            return True
