"""Redactor: secrets must go, evidence must stay.

Fake tokens are assembled at runtime so no token-shaped literal sits in the repo
(GitHub push protection would flag it, and it keeps scanners quiet).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from dagnose.models import Commit, ExceptionInfo, FailureContext, FailureJob, LogLine
from dagnose.redactor import Redactor, redact
from dagnose.redactor.detectors import looks_random
from tests.conftest import FIXTURES

R = "aB3xK9mQ2pL7vT4wZ8nY6cD1"  # random-looking filler for fake tokens


def ctx(*messages: str, **fields: Any) -> FailureContext:
    return FailureContext(
        job=FailureJob(dag_id="d", run_id="manual__2026-11-23T10:00:00+00:00", task_id="t"),
        log_tail=[LogLine(n=i + 1, message=m) for i, m in enumerate(messages)],
        collected_at=datetime(2026, 11, 23, tzinfo=timezone.utc),
        **fields,
    )


def redacted_log(*messages: str, **kwargs: Any) -> list[str]:
    return [line.message for line in redact(ctx(*messages), **kwargs).log_tail]


# ---- known formats ------------------------------------------------------------------

FORMATS = [
    ("aws_access_key", "AKIA" + "Q3EGRM7ZK2XW5HNP"),
    ("github_token", "ghp_" + R + "0123456789AB"),
    ("slack_token", "xoxb-" + "1234567890-" + R),
    ("slack_webhook", "https://hooks.slack.com/services/" + "T000/B000/" + R),
    ("stripe_key", "sk_" + "live_" + R),
    ("google_api_key", "AIza" + R + "abcdefghijk"),
    ("llm_api_key", "sk-" + "ant-" + R),
    ("jwt", "eyJhbGciOiJIUzI1NiJ9" + ".eyJzdWIiOiJ4In0" + "." + R),
]


@pytest.mark.parametrize(("kind", "secret"), FORMATS, ids=[k for k, _ in FORMATS])
def test_known_token_formats(kind: str, secret: str) -> None:
    [line] = redacted_log(f"calling api with {secret} now")
    assert secret not in line
    assert line == f"calling api with [REDACTED:{kind}#1] now"


def test_private_key_block() -> None:
    marker = "RSA " + "PRIVATE KEY-----"  # split so the repo holds no key-shaped literal
    key = "-----BEGIN " + marker + "\nMIIEow" + R + "\n-----END " + marker
    [line] = redacted_log(f"loaded {key} ok")
    assert line == "loaded [REDACTED:private_key#1] ok"


# ---- credentials in context ---------------------------------------------------------


def test_connection_string_keeps_user_and_host() -> None:
    [line] = redacted_log("connect postgresql://etl:pa55-w0rd@db.internal:5432/sales failed")
    assert line == "connect postgresql://etl:[REDACTED:password#1]@db.internal:5432/sales failed"


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("password=hunter2 user=bob", "password=[REDACTED:secret#1] user=bob"),
        ('{"api_key": "abc123xyz"}', '{"api_key": "[REDACTED:secret#1]"}'),
        ("{'token': 'abc123xyz'}", "{'token': '[REDACTED:secret#1]'}"),
        ("db_password: s3cr3t", "db_password: [REDACTED:secret#1]"),
        (
            "GET /x?region=eu&access_token=abc123&page=2",
            "GET /x?region=eu&access_token=[REDACTED:secret#1]&page=2",
        ),
        ("Authorization: Bearer " + R, "Authorization: Bearer [REDACTED:token#1]"),
        ("client_secret='x9y8z7'", "client_secret='[REDACTED:secret#1]'"),
    ],
)
def test_key_value_secrets(text: str, expected: str) -> None:
    assert redacted_log(text) == [expected]


@pytest.mark.parametrize(
    "text",
    [
        "password=None",
        "token: ***",
        "api_key={{ var.value.crm_key }}",
        "password=[REDACTED:secret#1]",
        "tokens=5 passwords_checked=3",
        "signal=SIGTERM",
    ],
)
def test_not_secrets_are_kept(text: str) -> None:
    assert redacted_log(text) == [text]


def test_email() -> None:
    assert redacted_log("paging data-oncall@example.com") == ["paging [REDACTED:email#1]"]


# ---- generic high entropy ---------------------------------------------------------


def test_unlabelled_random_token() -> None:
    assert redacted_log(f"key {R} rejected") == ["key [REDACTED:high_entropy#1] rejected"]


@pytest.mark.parametrize(
    "token",
    [
        "01a203e5-5607-7a6f-ab49-0ba768892e74",  # task instance id (UUID)
        "4ac023c1f2b6d8e9a0b1c2d3e4f5a6b7c8d9e0f1",  # git SHA
        "AirflowNotFoundException",  # class name
        "_PythonDecoratedOperator",
        "site-packages/airflow/providers/standard/operators",  # path
        "manual__2026-11-22T15:34:51",  # run id
        "TAX_RATES_BY_COUNTRY_2026_V2",  # constant
    ],
)
def test_evidence_is_not_random(token: str) -> None:
    assert not looks_random(token)
    assert redacted_log(f"value {token} here") == [f"value {token} here"]


# ---- source code ------------------------------------------------------------------


def test_code_redacts_hardcoded_secrets_but_keeps_expressions() -> None:
    source = (
        'password = Variable.get("warehouse_password")\n'
        'api_key = "abc123xyz"\n'
        "DSN = 'postgresql://etl:pa55@db:5432/x'\n"
    )
    out = redact(ctx(dag_source=source)).dag_source
    assert out == (
        'password = Variable.get("warehouse_password")\n'
        'api_key = "[REDACTED:secret#1]"\n'
        "DSN = 'postgresql://etl:[REDACTED:password#1]@db:5432/x'\n"
    )


# ---- structure ----------------------------------------------------------------------


def test_same_value_same_placeholder_everywhere() -> None:
    c = ctx(
        "connect postgresql://u:pw-one@h/db",
        "retry postgresql://u:pw-one@h/db",
        "other postgresql://u:pw-two@h/db",
        exceptions=[
            ExceptionInfo(type="OperationalError", message="auth postgresql://u:pw-one@h/db")
        ],
    )
    r = redact(c)
    assert [line.message for line in r.log_tail] == [
        "connect postgresql://u:[REDACTED:password#1]@h/db",
        "retry postgresql://u:[REDACTED:password#1]@h/db",
        "other postgresql://u:[REDACTED:password#2]@h/db",
    ]
    assert r.exceptions[0].message == "auth postgresql://u:[REDACTED:password#1]@h/db"
    assert r.summary() == {"password": 2}
    assert {x.path for x in r.redactions} == {
        "log_tail[0].message",
        "log_tail[1].message",
        "log_tail[2].message",
        "exceptions[0].message",
    }


def test_task_params_redact_by_key() -> None:
    c = ctx(
        task_params={
            "op_kwargs": {"api_token": "plain", "pin_code": 1234, "region": "eu", "dry_run": False},
            "templates_dict": {"warehouse": {"password": 9876, "host": "db.internal"}},
        },
        run_conf={"client_secret": "abc", "note": "rerun"},
    )
    r = redact(c)
    assert r.task_params["op_kwargs"] == {
        "api_token": "[REDACTED:secret_param#1]",
        "pin_code": 1234,
        "region": "eu",
        "dry_run": False,
    }
    assert r.task_params["templates_dict"] == {
        "warehouse": {"password": "[REDACTED:secret_param#2]", "host": "db.internal"}
    }
    assert r.run_conf == {"client_secret": "[REDACTED:secret_param#3]", "note": "rerun"}


def test_identifiers_are_never_touched() -> None:
    c = ctx("ok")
    c.job = FailureJob(dag_id="d", run_id="manual__" + R, task_id="t")
    assert redact(c).job == c.job


def test_git_authors_are_redacted_everywhere() -> None:
    commit = Commit(sha="a" * 40, author="Priya Raman", date=datetime(2026, 11, 1), subject="fix")
    r = redact(ctx("deployed by Priya Raman", recent_commits=[commit]))
    assert r.recent_commits[0].author == "[REDACTED:person#1]"
    assert r.log_tail[0].message == "deployed by [REDACTED:person#1]"
    assert r.recent_commits[0].sha == "a" * 40
    kept = redact(ctx("deployed by Priya Raman", recent_commits=[commit]), redact_people=False)
    assert kept.recent_commits[0].author == "Priya Raman"


def test_redaction_records_never_hold_the_value() -> None:
    secret = "pa55-w0rd-" + R
    r = redact(ctx(f"postgresql://u:{secret}@h/db", f"api_key={secret}"))
    assert secret not in r.model_dump_json()
    assert secret not in json.dumps([x.model_dump() for x in r.redactions])


def test_idempotent() -> None:
    once = redact(ctx("password=hunter2", f"key {R}", "mail me@example.com"))
    twice = redact(once)
    assert [line.message for line in twice.log_tail] == [line.message for line in once.log_tail]
    assert twice.redactions == []


def test_allow_and_extra_patterns() -> None:
    redactor = Redactor(extra_patterns=[r"CUST-\d{6}"], allow_patterns=[r"^ops@example\.com$"])
    r = redactor.redact(ctx("customer CUST-123456 mailed ops@example.com and bob@example.com"))
    assert r.log_tail[0].message == (
        "customer [REDACTED:custom#1] mailed ops@example.com and [REDACTED:email#1]"
    )


# ---- real recorded failures ---------------------------------------------------------

EXPECTED = {
    k: v
    for k, v in json.loads(
        (FIXTURES.parent.parent / "dev/dags/demo/expected.json").read_text()
    ).items()
    if not k.startswith("_")
}
RECORDED = sorted((FIXTURES / "contexts").glob("*/*.json"))


@pytest.mark.parametrize("path", RECORDED, ids=lambda p: f"{p.parent.name}-{p.stem}")
def test_recorded_failures(path: Path) -> None:
    raw = FailureContext.model_validate_json(path.read_text())
    r = redact(raw)
    expected = EXPECTED[path.stem]
    secrets = expected.get("must_not_leak", [])

    for secret in secrets:
        assert secret in raw.model_dump_json(), "fixture should contain the planted secret"
        assert secret not in r.model_dump_json(), "planted secret leaked"

    # Evidence survives: exception type, failing frame, log length, DAG file.
    assert r.exception is not None and raw.exception is not None
    assert r.exception.type == raw.exception.type == expected["exception"]
    assert r.exception.frames == raw.exception.frames
    assert len(r.log_tail) == len(raw.log_tail)
    assert r.dag_file == raw.dag_file
    assert r.run_history == raw.run_history

    if secrets:
        assert set(r.summary()) - {"person"}, "planted secrets but nothing redacted"
        assert "warehouse.internal:5432/sales" in r.exception.message  # host kept
    else:
        # No secrets in these failures: nothing but author names may change.
        assert set(r.summary()) <= {"person"}, r.summary()
        assert r.exception == raw.exception
        assert r.dag_source == raw.dag_source
        assert [x.message for x in r.log_tail] == [x.message for x in raw.log_tail]


def test_secret_seen_once_is_replaced_everywhere() -> None:
    """A password found inside a URL is also removed where it appears bare, even in an
    earlier field, and even in later text redacted with the same session."""
    secret = "Sup3r-S3cret-Pw"
    c = ctx(
        f"login failed, password {secret} rejected",  # no label: no pattern matches this
        f"connect postgresql://etl:{secret}@db:5432/x",  # found here
    )
    session = Redactor().session()
    r = session.redact(c)
    assert [x.message for x in r.log_tail] == [
        "login failed, password [REDACTED:password#1] rejected",
        "connect postgresql://etl:[REDACTED:password#1]@db:5432/x",
    ]
    assert session.redact_text(f"retry with {secret}", "tool") == "retry with [REDACTED:password#1]"


def test_ordinary_words_are_not_replaced_by_value() -> None:
    """api_token="default" must not blank out every "default" in the logs."""
    c = ctx("using default settings", task_params={"op_kwargs": {"api_token": "default"}})
    r = redact(c)
    assert r.task_params["op_kwargs"]["api_token"] == "[REDACTED:secret_param#1]"
    assert r.log_tail[0].message == "using default settings"
