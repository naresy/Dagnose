"""The diagnosis loop: prompt -> (tool calls)* -> structured answer -> evidence check.

Only a RedactedContext can be diagnosed (enforced by the type), and tool results are
redacted by the ToolBox, so nothing unredacted reaches the provider.

Evidence check: every quote the model cites is looked up in the context (or in the
tool results it saw). Evidence that is not found is kept but marked unverified, and
confidence is capped, so a plausible-sounding guess cannot pass as a proven diagnosis.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone

from pydantic import ValidationError

from airflow_doctor.agent.prompt import (
    DIAGNOSIS_SCHEMA,
    SYSTEM_PROMPT,
    history_row,
    render_context,
)
from airflow_doctor.agent.provider import LLMProvider, ToolResult
from airflow_doctor.agent.tools import ToolBox
from airflow_doctor.models import (
    Diagnosis,
    DiagnosisRun,
    RedactedContext,
    ToolCallRecord,
    Usage,
)

LAST_TURN_NOTE = (
    "No more tool calls are available. Give your final diagnosis now, based on what you have."
)
NO_EVIDENCE_CAP = 0.3  # nothing the model cited could be found
WEAK_EVIDENCE_CAP = 0.6  # less than half could be found


class DiagnosisError(RuntimeError):
    """The model did not produce a usable diagnosis (refusal, bad output, no answer)."""


def diagnose(
    ctx: RedactedContext,
    provider: LLMProvider,
    toolbox: ToolBox,
    *,
    max_turns: int = 6,
) -> DiagnosisRun:
    started = time.monotonic()
    session = provider.start(
        system=SYSTEM_PROMPT,
        prompt=render_context(ctx),
        tools=toolbox.specs,
        output_schema=DIAGNOSIS_SCHEMA,
    )
    usage = Usage()
    calls: list[ToolCallRecord] = []
    tool_outputs: list[str] = []
    results: list[ToolResult] = []
    served_model: str | None = None

    for turn_no in range(1, max_turns + 1):
        last = turn_no == max_turns
        turn = session.send(results, note=LAST_TURN_NOTE if last and results else None)
        usage = usage + turn.usage
        served_model = turn.served_model or served_model

        if turn.stop == "continue":
            results = []
            continue
        if turn.stop == "refusal":
            raise DiagnosisError(f"the model declined (category: {turn.refusal_category})")
        if turn.stop == "max_tokens":
            raise DiagnosisError("the answer was cut off (max_tokens)")
        if turn.stop == "tool_use" and turn.tool_calls:
            if last:
                raise DiagnosisError(f"no final answer after {max_turns} turns")
            results = [toolbox.run(call) for call in turn.tool_calls]
            for call, result in zip(turn.tool_calls, results, strict=True):
                calls.append(
                    ToolCallRecord(name=call.name, input=call.input, is_error=result.is_error)
                )
                if not result.is_error:
                    tool_outputs.append(result.content)
            continue

        diagnosis = parse_diagnosis(turn.text)
        model_confidence = diagnosis.confidence
        verified, total = verify_evidence(diagnosis, ctx, tool_outputs)
        capped = cap_confidence(diagnosis, verified, total)
        return DiagnosisRun(
            job=ctx.job,
            diagnosis=diagnosis,
            provider=provider.name,
            model=provider.model,
            served_model=served_model,
            turns=turn_no,
            tool_calls=calls,
            usage=usage,
            duration_s=round(time.monotonic() - started, 3),
            evidence_verified=verified,
            evidence_total=total,
            confidence_capped=capped,
            model_confidence=model_confidence,
            created_at=datetime.now(timezone.utc),
        )
    raise DiagnosisError(f"no final answer after {max_turns} turns")


def parse_diagnosis(text: str) -> Diagnosis:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise DiagnosisError(f"the answer is not JSON: {exc}") from exc
    if isinstance(data, dict) and isinstance(data.get("confidence"), (int, float)):
        data["confidence"] = min(max(float(data["confidence"]), 0.0), 1.0)
    try:
        return Diagnosis.model_validate(data)
    except ValidationError as exc:
        raise DiagnosisError(f"the answer does not match the schema: {exc}") from exc


# ---- evidence -----------------------------------------------------------------------

_WS = re.compile(r"\s+")


def _norm(text: str) -> str:
    return _WS.sub(" ", text).strip().lower()


def _found(quote: str, haystack: str) -> bool:
    q = _norm(quote)
    return bool(q) and q in _norm(haystack)


def verify_evidence(
    diagnosis: Diagnosis, ctx: RedactedContext, tool_outputs: list[str]
) -> tuple[int, int]:
    """Mark each evidence item verified if its quote is where it says. Returns counts."""
    log_by_n = {line.n: line.message for line in ctx.log_tail}
    source_lines = (ctx.dag_source or "").splitlines()
    exceptions = "\n".join(
        f"{e.type}: {e.message}\n"
        + "\n".join(f"{f.filename}:{f.lineno} in {f.name}" for f in e.frames)
        for e in ctx.exceptions
    )
    params = json.dumps({"task_params": ctx.task_params, "run_conf": ctx.run_conf}, default=str)
    history = "\n".join(history_row(r) for r in ctx.run_history)
    commits = "\n".join(f"{c.sha} {c.subject}\n{c.diff}" for c in ctx.recent_commits)
    tools = "\n".join(tool_outputs)

    for ev in diagnosis.evidence:
        if ev.source == "log":
            ev.verified = ev.line in log_by_n and _found(ev.quote, log_by_n[ev.line])
        elif ev.source == "dag_source":
            ev.verified = (
                ev.line is not None
                and 1 <= ev.line <= len(source_lines)
                and _found(ev.quote, source_lines[ev.line - 1])
            )
        elif ev.source == "exception":
            ev.verified = _found(ev.quote, exceptions)
        elif ev.source == "task_params":
            ev.verified = _found(ev.quote, params)
        elif ev.source == "run_history":
            ev.verified = _found(ev.quote, history)
        elif ev.source == "commit":
            ev.verified = _found(ev.quote, commits)
        elif ev.source == "tool_result":
            ev.verified = _found(ev.quote, tools)
    verified = sum(ev.verified for ev in diagnosis.evidence)
    return verified, len(diagnosis.evidence)


def cap_confidence(diagnosis: Diagnosis, verified: int, total: int) -> bool:
    """Lower confidence when the evidence does not check out. Returns True if capped."""
    if verified == 0:
        cap = NO_EVIDENCE_CAP
    elif verified * 2 < total:
        cap = WEAK_EVIDENCE_CAP
    else:
        return False
    if diagnosis.confidence > cap:
        diagnosis.confidence = cap
        return True
    return False
