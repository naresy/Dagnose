from __future__ import annotations

from airflow_doctor.collector.logs import parse_structured, parse_text


def test_structured_skips_ui_markers_and_source_header() -> None:
    parsed = parse_structured(
        [
            {"event": "::group::Log message source details"},
            {"event": "/opt/airflow/logs/dag_id=x/attempt=1.log"},
            {"event": "::endgroup::"},
            {"event": "hello", "level": "info", "logger": "task.stdout", "timestamp": "t1"},
            {"event": "::group::Post Execute", "level": "info", "logger": "task"},
        ]
    )
    assert [(line.n, line.message) for line in parsed.lines] == [(1, "hello")]
    assert parsed.lines[0].logger == "task.stdout"


def test_structured_exception_chain_is_root_cause_first() -> None:
    # Airflow (structlog) lists the raised exception first, then its cause.
    parsed = parse_structured(
        [
            {
                "event": "Task failed with exception",
                "level": "error",
                "error_detail": [
                    {"exc_type": "ValueError", "exc_value": "outer", "frames": []},
                    {
                        "exc_type": "KeyError",
                        "exc_value": "'x'",
                        "is_cause": True,
                        "frames": [{"filename": "/dags/a.py", "lineno": 3, "name": "f"}],
                    },
                ],
            }
        ]
    )
    assert [e.type for e in parsed.exceptions] == ["KeyError", "ValueError"]
    assert parsed.exceptions[0].frames[0].in_user_code


def test_tail_keeps_line_numbers() -> None:
    parsed = parse_structured([{"event": f"line {i}", "level": "info"} for i in range(10)])
    assert [line.n for line in parsed.tail(3)] == [8, 9, 10]
    assert parsed.tail(0) == []


AIRFLOW2_LOG = """\
[2026-11-22, 15:34:54 UTC] {taskinstance.py:1157} INFO - Dependencies all met
[2026-11-22, 15:34:54 UTC] {logging_mixin.py:190} INFO - Fetching orders
[2026-11-22, 15:34:54 UTC] {taskinstance.py:3311} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 767, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
  File "/opt/airflow/dags/orders.py", line 24, in compute_tax
    return order["amount"] * TAX_RATES[order["country"]]
                                       ~~~~~^^^^^^^^^^^
KeyError: 'country'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/opt/airflow/dags/orders.py", line 30, in wrapper
    raise RuntimeError("tax step failed") from e
RuntimeError: tax step failed
[2026-11-22, 15:34:54 UTC] {taskinstance.py:1225} INFO - Marking task as FAILED.
"""


def test_text_log_lines_and_levels() -> None:
    parsed = parse_text(AIRFLOW2_LOG)
    first = parsed.lines[0]
    assert (first.n, first.level, first.logger, first.timestamp) == (
        1,
        "info",
        "taskinstance.py:1157",
        "2026-11-22, 15:34:54 UTC",
    )
    assert first.message == "Dependencies all met"
    traceback_line = next(line for line in parsed.lines if line.message.startswith("Traceback"))
    assert traceback_line.level == "error"  # continuation lines keep the header level
    assert parsed.lines[-1].message == "Marking task as FAILED."


def test_text_log_chained_traceback() -> None:
    parsed = parse_text(AIRFLOW2_LOG)
    assert [(e.type, e.message) for e in parsed.exceptions] == [
        ("KeyError", "'country'"),
        ("RuntimeError", "tax step failed"),
    ]
    root = parsed.exceptions[0]
    assert [(f.filename.split("/")[-1], f.lineno) for f in root.frames] == [
        ("taskinstance.py", 767),
        ("orders.py", 24),
    ]
    assert [f.in_user_code for f in root.frames] == [False, True]


def test_text_log_keeps_only_the_last_failure() -> None:
    log = (
        "Traceback (most recent call last):\n"
        '  File "/dags/a.py", line 1, in f\n'
        "ValueError: first attempt\n"
        "[2026-11-22, 15:34:54 UTC] {x.py:1} INFO - retrying\n"
        "Traceback (most recent call last):\n"
        '  File "/dags/a.py", line 2, in f\n'
        "TimeoutError\n"
    )
    assert [(e.type, e.message) for e in parse_text(log).exceptions] == [("TimeoutError", "")]


def test_text_log_without_traceback() -> None:
    parsed = parse_text("[2026-11-22, 15:34:54 UTC] {x.py:1} ERROR - Process exited -9\n")
    assert parsed.exceptions == []
    assert parsed.lines[0].level == "error"
