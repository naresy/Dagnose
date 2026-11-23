"""Diagnosis agent, tested offline with a scripted model (no API key, no cost)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from dagnose.agent import (
    DiagnosisError,
    ProviderConfigError,
    ToolBox,
    diagnose,
    make_provider,
)
from dagnose.agent.diagnose import LAST_TURN_NOTE, cap_confidence, verify_evidence
from dagnose.agent.prompt import DIAGNOSIS_SCHEMA, SYSTEM_PROMPT, render_context
from dagnose.agent.provider import ToolCall, ToolResult, ToolSpec, Turn
from dagnose.collector import AirflowClient
from dagnose.models import (
    Diagnosis,
    Evidence,
    FailureContext,
    FailureJob,
    LogLine,
    RedactedContext,
    RunSummary,
    Usage,
)
from dagnose.redactor import Redactor
from tests.conftest import FIXTURES

LEAKY = FIXTURES / "contexts" / "v2" / "demo_crm_session_sync.json"
KEYERROR = FIXTURES / "contexts" / "v2" / "demo_orders_tax.json"
SECRETS = ["Sup3r-S3cret-Pw", "data-oncall@example.com", "ZmFrZS1zaWduYXR1cmUtZm9yLXRlc3Rz"]


class ScriptedProvider:
    """Plays back a fixed list of turns and records what the loop sent."""

    name = "scripted"
    model = "scripted-model"

    def __init__(self, turns: list[Turn]) -> None:
        self.turns = list(turns)
        self.started: dict[str, Any] = {}
        self.sent: list[tuple[list[ToolResult], str | None]] = []

    def start(
        self, *, system: str, prompt: str, tools: Sequence[ToolSpec], output_schema: dict[str, Any]
    ) -> ScriptedProvider:
        self.started = {
            "system": system,
            "prompt": prompt,
            "tools": list(tools),
            "schema": output_schema,
        }
        return self

    def send(self, tool_results: Sequence[ToolResult] = (), note: str | None = None) -> Turn:
        self.sent.append((list(tool_results), note))
        return self.turns.pop(0)


def answer(**overrides: Any) -> Turn:
    data: dict[str, Any] = {
        "summary": "compute_tax reads a 'country' key the upstream payload does not have.",
        "category": "code_bug",
        "root_cause": "extract_orders returns no 'country' field; compute_tax indexes it.",
        "evidence": [
            {"source": "dag_source", "line": 26, "quote": 'TAX_RATES[order["country"]]'},
            {"source": "exception", "line": None, "quote": "KeyError: 'country'"},
        ],
        "confidence": 0.9,
        "suggested_fix": "Add 'country' to the extract_orders payload.",
        "patch": None,
    }
    data.update(overrides)
    return Turn(
        stop="done",
        text=json.dumps(data),
        usage=Usage(input_tokens=100, output_tokens=50),
        served_model="m1",
    )


def load(path: Path) -> tuple[RedactedContext, Any]:
    session = Redactor().session()
    return session.redact(FailureContext.model_validate_json(path.read_text())), session


# ---- prompt -------------------------------------------------------------------------


def test_prompt_numbers_lines_and_holds_no_secrets() -> None:
    ctx, _ = load(LEAKY)
    prompt = render_context(ctx)
    for secret in SECRETS:
        assert secret not in prompt
    assert "[REDACTED:password#1]" in prompt
    assert "  29 | " in prompt  # DAG source line numbers, as evidence cites them
    assert f"[{ctx.log_tail[0].n}] " in prompt  # log line numbers
    assert "<- DAG code" in prompt and "<- this failure" in prompt


def test_schema_is_strict() -> None:
    assert DIAGNOSIS_SCHEMA["additionalProperties"] is False
    assert set(DIAGNOSIS_SCHEMA["required"]) == set(DIAGNOSIS_SCHEMA["properties"])
    assert set(DIAGNOSIS_SCHEMA["properties"]) == set(Diagnosis.model_fields)
    assert "REDACTED" in SYSTEM_PROMPT  # the model is told what placeholders mean


# ---- loop ---------------------------------------------------------------------------


def test_diagnose_with_a_tool_call() -> None:
    ctx, session = load(KEYERROR)
    call = ToolCall(id="c1", name="unknown_tool", input={})
    provider = ScriptedProvider(
        [
            Turn(stop="tool_use", tool_calls=[call], usage=Usage(input_tokens=10, output_tokens=5)),
            answer(),
        ]
    )
    run = diagnose(ctx, provider, ToolBox(ctx, None, session))

    assert run.turns == 2
    assert [c.name for c in run.tool_calls] == ["unknown_tool"] and run.tool_calls[0].is_error
    assert provider.sent[1][0][0].call_id == "c1" and provider.sent[1][0][0].is_error
    assert run.usage.input_tokens == 110 and run.usage.output_tokens == 55
    assert run.served_model == "m1"
    assert run.diagnosis.category == "code_bug"
    assert (run.evidence_verified, run.evidence_total) == (2, 2)
    assert run.diagnosis.confidence == 0.9 and not run.confidence_capped


def test_last_turn_forces_an_answer() -> None:
    ctx, session = load(KEYERROR)
    call = ToolCall(id="c1", name="x", input={})
    provider = ScriptedProvider([Turn(stop="tool_use", tool_calls=[call]), answer()])
    diagnose(ctx, provider, ToolBox(ctx, None, session), max_turns=2)
    assert provider.sent[1][1] == LAST_TURN_NOTE


def test_no_answer_within_turn_limit() -> None:
    ctx, session = load(KEYERROR)
    call = ToolCall(id="c1", name="x", input={})
    provider = ScriptedProvider([Turn(stop="tool_use", tool_calls=[call])] * 2)
    with pytest.raises(DiagnosisError, match="no final answer"):
        diagnose(ctx, provider, ToolBox(ctx, None, session), max_turns=2)


@pytest.mark.parametrize(
    ("turn", "message"),
    [
        (Turn(stop="refusal", refusal_category="cyber"), "declined"),
        (Turn(stop="max_tokens"), "cut off"),
        (Turn(stop="done", text="not json"), "not JSON"),
        (Turn(stop="done", text=json.dumps({"summary": "x"})), "schema"),
    ],
)
def test_unusable_answers(turn: Turn, message: str) -> None:
    ctx, session = load(KEYERROR)
    with pytest.raises(DiagnosisError, match=message):
        diagnose(ctx, ScriptedProvider([turn]), ToolBox(ctx, None, session))


def test_out_of_range_confidence_is_clamped() -> None:
    ctx, session = load(KEYERROR)
    run = diagnose(ctx, ScriptedProvider([answer(confidence=1.7)]), ToolBox(ctx, None, session))
    assert run.diagnosis.confidence == 1.0


# ---- evidence -----------------------------------------------------------------------


def _ctx() -> RedactedContext:
    return RedactedContext(
        job=FailureJob(dag_id="d", run_id="r", task_id="t"),
        log_tail=[
            LogLine(n=7, message="Task failed with exception"),
            LogLine(n=8, message="KeyError: 'country'"),
        ],
        dag_source="line one\nreturn TAX_RATES[order['country']]\n",
        task_params={"op_args": [{"amount": 19.99}]},
        run_history=[RunSummary(run_id="r", state="failed", is_current=True)],
        collected_at=datetime(2026, 11, 23, tzinfo=timezone.utc),
    )


@pytest.mark.parametrize(
    ("evidence", "verified"),
    [
        (
            Evidence(source="log", line=8, quote="KeyError:   'country'"),
            True,
        ),  # whitespace-tolerant
        (Evidence(source="log", line=7, quote="KeyError"), False),  # right text, wrong line
        (Evidence(source="log", line=99, quote="KeyError"), False),
        (Evidence(source="dag_source", line=2, quote="TAX_RATES[order['country']]"), True),
        (Evidence(source="dag_source", line=1, quote="TAX_RATES"), False),
        (Evidence(source="task_params", quote='"amount": 19.99'), True),
        (Evidence(source="run_history", quote="r | failed"), True),  # as rendered in the prompt
        (Evidence(source="commit", quote="anything"), False),
        (Evidence(source="tool_result", quote="extract_orders success"), True),
        (Evidence(source="log", line=8, quote="   "), False),  # empty quote never verifies
    ],
)
def test_verify_evidence(evidence: Evidence, verified: bool) -> None:
    d = Diagnosis(
        summary="s", category="code_bug", root_cause="r", evidence=[evidence], confidence=0.9
    )
    verify_evidence(d, _ctx(), ["extract_orders  success"])
    assert d.evidence[0].verified is verified


@pytest.mark.parametrize(
    ("verified", "total", "confidence", "expected", "capped"),
    [
        (0, 2, 0.9, 0.3, True),
        (0, 0, 0.9, 0.3, True),  # no evidence at all
        (1, 3, 0.9, 0.6, True),
        (2, 3, 0.9, 0.9, False),
        (0, 2, 0.2, 0.2, False),  # already low
    ],
)
def test_confidence_cap(
    verified: int, total: int, confidence: float, expected: float, capped: bool
) -> None:
    d = Diagnosis(summary="s", category="unknown", root_cause="r", confidence=confidence)
    assert cap_confidence(d, verified, total) is capped
    assert d.confidence == expected


def test_made_up_evidence_lowers_confidence() -> None:
    ctx, session = load(KEYERROR)
    fake = [{"source": "log", "line": 3, "quote": "disk full on worker"}]
    run = diagnose(ctx, ScriptedProvider([answer(evidence=fake)]), ToolBox(ctx, None, session))
    assert run.evidence_verified == 0 and run.confidence_capped
    assert run.model_confidence == 0.9 and run.diagnosis.confidence == 0.3


# ---- tools --------------------------------------------------------------------------


def _airflow(handler: Any, make_settings: Any) -> AirflowClient:
    return AirflowClient(
        make_settings(airflow_api="v2", airflow_token="t"), transport=httpx.MockTransport(handler)
    )


def test_tools_redact_with_the_same_placeholders(make_settings: Any) -> None:
    ctx, session = load(LEAKY)
    leaked = "postgresql://etl_user:Sup3r-S3cret-Pw@warehouse.internal:5432/sales"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/logs/1"):
            events = [
                {
                    "event": f"retrying {leaked} as data-oncall@example.com",
                    "level": "info",
                    "logger": "task",
                }
            ]
            return httpx.Response(200, json={"content": events})
        return httpx.Response(200, json={"try_number": 1})

    with _airflow(handler, make_settings) as client:
        box = ToolBox(ctx, client, session)
        result = box.run(
            ToolCall(
                "c1",
                "get_task_log",
                {"task_id": "sync_crm", "run_id": None, "try_number": None, "tail_lines": 50},
            )
        )
    assert not result.is_error
    for secret in SECRETS:
        assert secret not in result.content
    # Same placeholders as in the context, so the model can connect them.
    assert "[REDACTED:password#1]" in result.content and "[REDACTED:email#1]" in result.content


def test_get_task_log_only_reads_known_runs(make_settings: Any) -> None:
    ctx, session = load(KEYERROR)
    with _airflow(lambda r: httpx.Response(500), make_settings) as client:
        result = ToolBox(ctx, client, session).run(
            ToolCall(
                "c1",
                "get_task_log",
                {"task_id": "x", "run_id": "someone_elses_run", "try_number": 1, "tail_lines": 5},
            )
        )
    assert result.is_error and "not this run" in result.content


def test_list_run_tasks(make_settings: Any) -> None:
    ctx, session = load(KEYERROR)
    tis = {
        "task_instances": [
            {
                "task_id": "compute_tax",
                "state": "failed",
                "try_number": 1,
                "start_date": "2026-11-23T10:01",
                "operator": "_PythonDecoratedOperator",
            },
            {
                "task_id": "extract_orders",
                "state": "success",
                "try_number": 1,
                "start_date": "2026-11-23T10:00",
                "operator": "_PythonDecoratedOperator",
            },
        ]
    }
    with _airflow(lambda r: httpx.Response(200, json=tis), make_settings) as client:
        result = ToolBox(ctx, client, session).run(ToolCall("c1", "list_run_tasks", {}))
    rows = json.loads(result.content)
    assert [r["task_id"] for r in rows] == ["extract_orders", "compute_tax"]  # run order


def test_airflow_error_is_a_tool_error(make_settings: Any) -> None:
    ctx, session = load(KEYERROR)
    with _airflow(lambda r: httpx.Response(503), make_settings) as client:
        result = ToolBox(ctx, client, session).run(ToolCall("c1", "list_run_tasks", {}))
    assert result.is_error and "Airflow API" in result.content


def test_no_tools_without_airflow() -> None:
    ctx, session = load(KEYERROR)
    assert ToolBox(ctx, None, session).specs == ()


# ---- provider selection -------------------------------------------------------------


def test_make_provider(make_settings: Any) -> None:
    p = make_provider(make_settings(llm_model="anthropic/claude-opus-5-5", llm_api_key="k"))
    assert (p.name, p.model) == ("anthropic", "claude-opus-5-5")
    with pytest.raises(ProviderConfigError, match="not supported"):
        make_provider(make_settings(llm_model="openai/gpt-x"))


def test_history_quotes_from_the_prompt_verify() -> None:
    """Regression: the checker must compare against the history exactly as rendered."""
    ctx, _ = load(KEYERROR)
    prompt = render_context(ctx)
    row = next(line for line in prompt.splitlines() if "<- this failure" in line)
    quote = row.replace("  <- this failure", "")
    d = Diagnosis(
        summary="s",
        category="code_bug",
        root_cause="r",
        evidence=[Evidence(source="run_history", quote=quote)],
        confidence=0.9,
    )
    assert verify_evidence(d, ctx, []) == (1, 1)
