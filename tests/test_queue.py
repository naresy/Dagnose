from __future__ import annotations

import json
import multiprocessing
from pathlib import Path

from dagnose.queue import JobQueue, write_spool_job


def _job(n: int = 1, **extra: object) -> dict[str, object]:
    return {"dag_id": "d", "run_id": f"r{n}", "task_id": "t", "try_number": 1, **extra}


def test_enqueue_dedupes_same_task_try(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    assert q.enqueue("d", "r", "t", 1) is True
    assert q.enqueue("d", "r", "t", 1) is False
    assert q.enqueue("d", "r", "t", 2) is True  # a retry is a new job
    assert q.counts() == {"pending": 2}


def test_claim_is_fifo_and_marks_running(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    q.enqueue("d", "r1", "t", 1)
    q.enqueue("d", "r2", "t", 1)
    first = q.claim()
    assert first is not None and first.run_id == "r1" and first.attempts == 1
    assert q.counts() == {"pending": 1, "running": 1}
    second = q.claim()
    assert second is not None and second.run_id == "r2"
    assert q.claim() is None


def test_mark_error_with_retry_returns_job_to_pending(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    q.enqueue("d", "r", "t", 1)
    job = q.claim()
    assert job is not None
    q.mark_error(job.id, "airflow down", retry=True)
    again = q.claim()
    assert again is not None and again.id == job.id and again.attempts == 2
    q.mark_error(again.id, "still down", retry=False)
    assert q.counts() == {"error": 1}


def test_requeue_stale(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    q.enqueue("d", "r", "t", 1)
    assert q.claim() is not None
    assert q.requeue_stale(older_than_s=-1) == 1
    assert q.counts() == {"pending": 1}


def test_spool_roundtrip(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    write_spool_job(spool, _job(1, map_index=3, failed_at="2026-11-22T00:00:00+00:00"))
    q = JobQueue(tmp_path / "q.db")
    assert q.ingest_spool(spool) == 1
    assert list(spool.glob("*.json")) == []
    job = q.claim()
    assert job is not None
    assert (job.run_id, job.map_index, job.failed_at) == ("r1", 3, "2026-11-22T00:00:00+00:00")


def test_ingest_moves_bad_files_aside(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    spool.mkdir()
    (spool / "broken.json").write_text("{not json")
    (spool / "missing.json").write_text(json.dumps({"dag_id": "d"}))
    write_spool_job(spool, _job())
    q = JobQueue(tmp_path / "q.db")
    assert q.ingest_spool(spool) == 1
    assert sorted(p.name for p in (spool / "bad").iterdir()) == ["broken.json", "missing.json"]


def test_ingest_without_spool_dir(tmp_path: Path) -> None:
    assert JobQueue(tmp_path / "q.db").ingest_spool(tmp_path / "nope") == 0


def _spool_writer(args: tuple[str, int]) -> None:
    spool, n = args
    write_spool_job(Path(spool), _job(n))


def test_concurrent_spool_writers(tmp_path: Path) -> None:
    """Many Airflow task processes failing at once must not lose or corrupt jobs.
    (A shared SQLite file did exactly that over a Docker bind mount.)"""
    spool = tmp_path / "spool"
    with multiprocessing.get_context("spawn").Pool(8) as pool:
        pool.map(_spool_writer, [(str(spool), n) for n in range(40)])
    q = JobQueue(tmp_path / "q.db")
    assert q.ingest_spool(spool) == 40
    assert q.counts() == {"pending": 40}


def test_dedupe_when_try_number_is_missing(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    assert q.enqueue("d", "r", "t", None) is True
    assert q.enqueue("d", "r", "t", None) is False


def test_retry_waits_for_backoff(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    q.enqueue("d", "r", "t", 1)
    job = q.claim()
    assert job is not None
    q.mark_error(job.id, "airflow down", retry=True, retry_after_s=60)
    assert q.claim() is None  # not due yet
    assert q.counts() == {"pending": 1}


def test_upgrades_queue_from_older_schema(tmp_path: Path) -> None:
    import sqlite3

    path = tmp_path / "q.db"
    old = sqlite3.connect(path)
    old.execute(
        "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, dag_id TEXT NOT NULL,"
        " run_id TEXT NOT NULL, task_id TEXT NOT NULL, try_number INTEGER,"
        " map_index INTEGER NOT NULL DEFAULT -1, failed_at TEXT,"
        " status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,"
        " last_error TEXT, enqueued_at REAL NOT NULL, updated_at REAL NOT NULL,"
        " UNIQUE (dag_id, run_id, task_id, try_number, map_index))"
    )
    old.execute(
        "INSERT INTO jobs (dag_id, run_id, task_id, try_number, enqueued_at, updated_at)"
        " VALUES ('d', 'r', 't', 1, 0, 0)"
    )
    old.commit()
    old.close()
    q = JobQueue(path)
    job = q.claim()
    assert job is not None and job.run_id == "r"


def test_recent_lists_newest_first_and_filters_by_dag(tmp_path: Path) -> None:
    q = JobQueue(tmp_path / "q.db")
    q.enqueue("d", "r1", "t", 1)
    q.enqueue("other", "r2", "t", 3, map_index=1)
    job = q.claim()
    assert job is not None
    q.mark_error(job.id, "airflow down", retry=False)
    rows = q.recent(10)
    assert [r["dag_id"] for r in rows] == ["d", "other"]  # the errored job was updated last
    assert rows[0]["status"] == "error" and rows[0]["last_error"] == "airflow down"
    assert [r["map_index"] for r in q.recent(10, dag_id="other")] == [1]
    assert len(q.recent(1)) == 1
