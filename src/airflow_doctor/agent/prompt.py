"""System prompt, output schema, and how a RedactedContext is shown to the model.

Line numbers in the rendered context are the ones evidence must cite: log lines use
LogLine.n, DAG source lines are 1-based file lines. Both are checked after the answer.
"""

from __future__ import annotations

import json
from typing import Any

from airflow_doctor.models import RedactedContext, RunSummary

SYSTEM_PROMPT = """\
You are Airflow Doctor, an on-call engineer for Apache Airflow. You get the context of
one failed task: the exception, the task log, the DAG source, task parameters, recent
runs of the same task and recent git commits to the DAG file. Work out why it failed
and how to fix it.

How to work:
- Base the diagnosis on evidence in the context or in tool results. Quote the exact
  text you rely on, with its log line number [n] or DAG source line number. Quotes are
  checked against the context; evidence that cannot be found lowers your confidence.
- Look past the last error line: the root cause is often earlier in the log, in an
  upstream task, in the task's input, or in a recent commit.
- Use the tools only when the context is not enough, for example to read an upstream
  task's log or the log of the last successful run.
- If the evidence does not support a confident answer, say so: use category "unknown"
  or a low confidence rather than a plausible guess.
- Values like [REDACTED:password#1] are secrets that were removed on purpose. The same
  placeholder always stands for the same value. Never ask for or try to recover them.

The answer:
- summary: one plain sentence a non-expert can act on.
- root_cause: the specific cause, not a restatement of the error message.
- category: code_bug, upstream_data, dependency, config, permissions, timeout, infra,
  or unknown.
- evidence: 1 to 6 items, each with source, line (log line n or DAG source line, null
  for other sources) and an exact quote.
- confidence: 0 to 1.
- suggested_fix: the smallest change that fixes it, or null.
- patch: a unified diff against the DAG file, only when the fix is a code change in
  that file and you are confident; otherwise null. Never put secrets in a patch.
"""

DIAGNOSIS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "category": {
            "type": "string",
            "enum": [
                "code_bug",
                "upstream_data",
                "dependency",
                "config",
                "permissions",
                "timeout",
                "infra",
                "unknown",
            ],
        },
        "root_cause": {"type": "string"},
        "evidence": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "enum": [
                            "log",
                            "dag_source",
                            "exception",
                            "task_params",
                            "run_history",
                            "commit",
                            "tool_result",
                        ],
                    },
                    "line": {"type": ["integer", "null"]},
                    "quote": {"type": "string"},
                },
                "required": ["source", "line", "quote"],
                "additionalProperties": False,
            },
        },
        "confidence": {"type": "number"},
        "suggested_fix": {"type": ["string", "null"]},
        "patch": {"type": ["string", "null"]},
    },
    "required": [
        "summary",
        "category",
        "root_cause",
        "evidence",
        "confidence",
        "suggested_fix",
        "patch",
    ],
    "additionalProperties": False,
}


def history_row(r: RunSummary) -> str:
    """One run_history line, exactly as the model sees it (and as evidence is checked)."""
    return f"{r.run_id} | {r.state} | {r.try_number} | {r.start_date} | {r.duration_s}"


def render_context(ctx: RedactedContext) -> str:
    """The user message: every part of the context the model may cite, with line numbers."""
    job = ctx.job
    parts: list[str] = [
        "<failure>",
        f"dag_id: {job.dag_id}",
        f"task_id: {job.task_id}",
        f"run_id: {job.run_id}",
        f"try_number: {job.try_number}"
        + (f", map_index: {job.map_index}" if job.map_index >= 0 else ""),
        f"airflow: {ctx.airflow_version} (REST API {ctx.api_version})",
        f"operator: {ctx.operator}",
        f"state: {ctx.task_state}, duration: {ctx.duration_s}s",
        "</failure>",
    ]

    parts.append("<exception>")
    if not ctx.exceptions:
        parts.append("(no exception found in the log)")
    for i, exc in enumerate(ctx.exceptions):
        label = "raised" if i == len(ctx.exceptions) - 1 else "caused by" if i == 0 else "then"
        parts.append(f"{label}: {exc.type}: {exc.message}")
        for f in exc.frames:
            mark = "  <- DAG code" if f.in_user_code else ""
            parts.append(f"  at {f.filename}:{f.lineno} in {f.name}{mark}")
        parts.extend(f"  note: {n}" for n in exc.notes)
    parts.append("</exception>")

    shown = len(ctx.log_tail)
    parts.append(
        f'<log lines="{shown} of {ctx.log_total_lines}" format="[n] level logger: message">'
    )
    for line in ctx.log_tail:
        parts.append(f"[{line.n}] {line.level or '-'} {line.logger or '-'}: {line.message}")
    parts.append("</log>")

    parts.append(f'<dag_source file="{ctx.dag_file}" format="line | code">')
    if ctx.dag_source:
        for n, code in enumerate(ctx.dag_source.splitlines(), start=1):
            parts.append(f"{n:4} | {code}")
    else:
        parts.append("(not available)")
    parts.append("</dag_source>")

    parts.append("<task_params>")
    parts.append(json.dumps(ctx.task_params, indent=1, sort_keys=True, default=str))
    parts.append("</task_params>")
    if ctx.run_conf:
        parts.append("<run_conf>")
        parts.append(json.dumps(ctx.run_conf, indent=1, sort_keys=True, default=str))
        parts.append("</run_conf>")

    parts.append(
        '<run_history format="run_id | state | try | start | duration" order="newest first">'
    )
    for r in ctx.run_history:
        current = "  <- this failure" if r.is_current else ""
        parts.append(history_row(r) + current)
    parts.append("</run_history>")

    parts.append('<recent_commits order="newest first">')
    if not ctx.recent_commits:
        parts.append("(none, or git history not configured)")
    for c in ctx.recent_commits:
        parts.append(f"commit {c.sha[:12]} {c.date.isoformat()} {c.author}: {c.subject}")
        if c.diff:
            parts.append(c.diff + ("\n(diff truncated)" if c.diff_truncated else ""))
    parts.append("</recent_commits>")

    if ctx.collection_errors:
        parts.append("<collection_problems>")
        parts.extend(ctx.collection_errors)
        parts.append("</collection_problems>")
    return "\n".join(parts)
