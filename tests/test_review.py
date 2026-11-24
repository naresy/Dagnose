"""`dagnose review`: grading diagnoses and the results report."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from dagnose import review as rv
from dagnose.cli import app
from dagnose.models import Diagnosis, DiagnosisRun, Evidence, FailureJob, Usage

FAILED = datetime(2026, 11, 24, 3, 7, tzinfo=timezone.utc)


def write_run(home: Path, name: str, *, category: str = "code_bug", confidence: float = 0.9,
              reused: bool = False, seconds: int = 40) -> Path:  # fmt: skip
    run = DiagnosisRun(
        job=FailureJob(dag_id=f"dag_{name}", run_id="r1", task_id="t", try_number=1, failed_at=FAILED),
        diagnosis=Diagnosis(
            summary="s", category=category, root_cause="rc", confidence=confidence,  # type: ignore[arg-type]
            evidence=[Evidence(source="log", line=3, quote="x", verified=True), Evidence(source="log", line=4, quote="y")],
            suggested_fix="fix it",
        ),  # fmt: skip
        provider="anthropic", model="claude-opus-5-5", turns=1, duration_s=9.0,
        usage=Usage(input_tokens=5000, output_tokens=1000),
        evidence_verified=1, evidence_total=2, model_confidence=confidence,
        created_at=FAILED + timedelta(seconds=seconds),
        reused_from="earlier" if reused else None, fingerprint=f"fp{name}",
    )  # fmt: skip
    path = home / "diagnoses" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(run.model_dump_json())
    return path


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("DAGNOSE_HOME", str(tmp_path))
    monkeypatch.chdir(tmp_path)  # no stray .env
    return tmp_path


def test_cost_and_latency(home: Path) -> None:
    run = DiagnosisRun.model_validate_json(write_run(home, "a").read_text())
    assert rv.estimate_cost(run) == pytest.approx((5000 * 4 + 1000 * 20) / 1e6)  # 0.04
    assert rv.latency_s(run) == 40


def test_review_flow(home: Path) -> None:
    write_run(home, "a")
    write_run(home, "b", category="config")
    write_run(home, "c", reused=True)  # a repeat: not reviewed again
    assert [p.name for p in rv.pending(home)] == ["a.json", "b.json"]

    answers = "y\nc\nu\n\n\n" + "n\nw\nw\nwrong connection id\nfound in run history\n"
    result = CliRunner().invoke(app, ["review"], input=answers)
    assert result.exit_code == 0, result.output
    assert "2 diagnosis(es) to review" in result.output and "[found] log:3" in result.output
    saved = rv.load_reviews(home)
    assert [(r.dag_id, r.category_correct, r.root_cause, r.fix) for r in saved] == [
        ("dag_a", "yes", "correct", "useful"),
        ("dag_b", "no", "wrong", "wrong"),
    ]
    assert saved[1].real_cause == "wrong connection id" and saved[1].cost_usd == pytest.approx(0.04)
    assert rv.pending(home) == []


def test_skip_and_quit(home: Path) -> None:
    write_run(home, "a")
    write_run(home, "b")
    result = CliRunner().invoke(app, ["review"], input="s\nq\n")
    assert result.exit_code == 0
    assert rv.load_reviews(home) == [] and len(rv.pending(home)) == 2


def test_invalid_answer_is_asked_again(home: Path) -> None:
    write_run(home, "a")
    result = CliRunner().invoke(app, ["review"], input="maybe\ny\nc\nu\n\n\n")
    assert "please answer one of" in result.output and len(rv.load_reviews(home)) == 1


def test_report(home: Path, tmp_path: Path) -> None:
    def add(
        name: str, cat: str, root: str, fix: str, conf: float, seconds: int, real: str = ""
    ) -> None:
        path = write_run(home, name, confidence=conf, seconds=seconds)
        run = DiagnosisRun.model_validate_json(path.read_text())
        rv.save_review(
            home,
            rv.make_review(
                path,
                run,
                {
                    "category_correct": cat,
                    "root_cause": root,
                    "fix": fix,
                    "real_cause": real,
                    "notes": "",
                },
            ),
        )

    add("a", "yes", "correct", "useful", 0.9, 30)
    add("b", "yes", "correct", "partly", 0.8, 50)
    add("c", "no", "wrong", "wrong", 0.7, 90, real="upstream table late")
    add("d", "unsure", "partly", "none", 0.5, 40)
    text = rv.report(rv.load_reviews(home))
    assert "| Category correct | 67% (2/3) |" in text  # unsure ones are left out
    assert "| Root cause correct | 50% (2/4) |" in text
    assert "| Root cause correct or partly | 75% (3/4) |" in text
    assert "| Fix useful | 33% (1/3) |" in text  # 'none' (no fix given) left out
    assert "| Evidence quotes found in the logs | 50% (4/8) |" in text
    assert (
        "| Average confidence when right | 0.85 |" in text
        and "| Average confidence when wrong | 0.70 |" in text
    )
    assert "| Cost per diagnosis (average) | USD 0.040 |" in text
    assert "| dag_c.t | code_bug (0.70) | upstream table late |" in text

    out = tmp_path / "results.md"
    result = CliRunner().invoke(app, ["review", "--report", "--out", str(out)])
    assert result.exit_code == 0 and out.read_text().startswith("# Dagnose test results")


def test_report_without_reviews(home: Path) -> None:
    assert "No reviews yet" in rv.report([])
