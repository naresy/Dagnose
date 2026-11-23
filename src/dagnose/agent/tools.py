"""Read-only tools the diagnosis model may call.

Every result is redacted with the job's RedactionSession before the model sees it, so
the tools cannot become a way around the redactor. Tools are scoped to the failed
task's DAG: they can read other tasks of this run, and runs listed in the context's
run history (for example the last successful run), nothing else.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from dagnose.agent.provider import ToolCall, ToolResult, ToolSpec
from dagnose.collector.airflow_api import AirflowAPIError, AirflowClient
from dagnose.collector.logs import parse_log
from dagnose.models import FailureJob, RedactedContext
from dagnose.redactor import RedactionSession

MAX_RESULT_CHARS = 30_000

SPECS = (
    ToolSpec(
        name="list_run_tasks",
        description=(
            "List every task instance in the failed task's DAG run with its state, try "
            "number, duration and operator. Use it to find upstream tasks that failed, "
            "were skipped, or produced this task's input."
        ),
        input_schema={
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    ),
    ToolSpec(
        name="get_task_log",
        description=(
            "Read the log of a task in this DAG. task_id: any task of this DAG. run_id: "
            "null for the failed run, or a run_id from run_history (for example the last "
            "successful run, to compare). try_number: null for the latest try. "
            "tail_lines: how many of the last log lines to return (1-500)."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "run_id": {"type": ["string", "null"]},
                "try_number": {"type": ["integer", "null"]},
                "tail_lines": {"type": "integer"},
            },
            "required": ["task_id", "run_id", "try_number", "tail_lines"],
            "additionalProperties": False,
        },
    ),
)


class ToolError(Exception):
    """A tool call the model can recover from (bad argument, not found)."""


class ToolBox:
    def __init__(
        self,
        ctx: RedactedContext,
        client: AirflowClient | None,
        session: RedactionSession,
    ) -> None:
        self.ctx = ctx
        self.client = client
        self.session = session
        self._handlers: dict[str, Callable[[dict[str, Any]], str]] = {
            "list_run_tasks": self._list_run_tasks,
            "get_task_log": self._get_task_log,
        }

    @property
    def specs(self) -> tuple[ToolSpec, ...]:
        return SPECS if self.client is not None else ()

    def run(self, call: ToolCall) -> ToolResult:
        handler = self._handlers.get(call.name)
        try:
            if handler is None or self.client is None:
                raise ToolError(f"unknown tool {call.name!r}")
            raw = handler(call.input)
        except ToolError as exc:
            return ToolResult(call.id, f"Error: {exc}", is_error=True)
        except AirflowAPIError as exc:
            return ToolResult(call.id, f"Error: Airflow API: {exc}", is_error=True)
        text = self.session.redact_text(raw, f"tool:{call.name}")
        if len(text) > MAX_RESULT_CHARS:
            text = "(output truncated to the last part)\n" + text[-MAX_RESULT_CHARS:]
        return ToolResult(call.id, text)

    # ---- tools ------------------------------------------------------------------

    def _list_run_tasks(self, args: dict[str, Any]) -> str:
        assert self.client is not None
        tis = self.client.run_task_instances(self.ctx.job.dag_id, self.ctx.job.run_id)
        rows = [
            {
                "task_id": t.get("task_id"),
                "map_index": t.get("map_index"),
                "state": t.get("state"),
                "try_number": t.get("try_number"),
                "duration_s": t.get("duration"),
                "operator": t.get("operator"),
            }
            for t in sorted(tis, key=lambda t: (t.get("start_date") or "", t.get("task_id") or ""))
        ]
        return json.dumps(rows, indent=1)

    def _get_task_log(self, args: dict[str, Any]) -> str:
        assert self.client is not None
        task_id = str(args.get("task_id") or "").strip()
        if not task_id:
            raise ToolError("task_id is required")
        run_id = args.get("run_id") or self.ctx.job.run_id
        allowed_runs = {self.ctx.job.run_id} | {r.run_id for r in self.ctx.run_history}
        if run_id not in allowed_runs:
            raise ToolError(f"run_id {run_id!r} is not this run or one listed in run_history")
        tail = int(args.get("tail_lines") or 200)
        tail = max(1, min(tail, 500))

        job = FailureJob(dag_id=self.ctx.job.dag_id, run_id=run_id, task_id=task_id)
        try_number = args.get("try_number")
        if try_number is None:
            ti = self.client.task_instance(job)
            try_number = ti.get("try_number") or 1
        parsed = parse_log(self.client.task_log(job, int(try_number)))
        lines = parsed.tail(tail)
        header = (
            f"log of {task_id} (run {run_id}, try {try_number}): "
            f"last {len(lines)} of {len(parsed.lines)} lines"
        )
        body = [f"[{ln.n}] {ln.level or '-'} {ln.logger or '-'}: {ln.message}" for ln in lines]
        excs = [f"exception: {e.type}: {e.message}" for e in parsed.exceptions]
        return "\n".join([header, *excs, *body])
