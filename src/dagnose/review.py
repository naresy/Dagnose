"""Grade diagnoses by hand and measure the results: `dagnose review`.

Testing produces opinions unless every diagnosis is graded the same way. For each new
diagnosis, a person answers three questions (category right? root cause right? fix
useful?) and optionally writes the real cause. Answers are stored next to the
diagnoses in DAGNOSE_HOME/reviews.jsonl (local only: they describe your real failures),
and `dagnose review --report` turns them into accuracy, calibration, cost and speed.
Reused diagnoses (repeats of a failure) are not reviewed again.
"""

from __future__ import annotations

import json
import statistics
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dagnose.models import DiagnosisRun

# USD per million tokens: input, output, cache read, cache write. Estimates only.
PRICES: dict[str, tuple[float, float, float, float]] = {
    "claude-opus-5-5": (4.00, 20.00, 0.20, 5.00),
    "claude-sonnet-5-5": (2.00, 10.00, 0.10, 2.50),
    "claude-haiku-5-5": (0.10, 0.50, 0.01, 0.125),
}


def estimate_cost(run: DiagnosisRun) -> float | None:
    model = run.model.removeprefix("anthropic.").split("/")[-1]
    price = PRICES.get(model)
    if price is None:
        return None
    u = run.usage
    return (
        u.input_tokens * price[0]
        + u.output_tokens * price[1]
        + u.cache_read_input_tokens * price[2]
        + u.cache_creation_input_tokens * price[3]
    ) / 1e6


def latency_s(run: DiagnosisRun) -> float | None:
    """From the task failing to the diagnosis being ready."""
    if run.job.failed_at is None:
        return None
    return (run.created_at - run.job.failed_at).total_seconds()


@dataclass
class Review:
    diagnosis_file: str
    dag_id: str
    task_id: str
    run_id: str
    fingerprint: str | None
    category: str
    confidence: float
    evidence_verified: int
    evidence_total: int
    model: str
    cost_usd: float | None
    latency_s: float | None
    category_correct: str  # yes | no | unsure
    root_cause: str  # correct | partly | wrong
    fix: str  # useful | partly | wrong | none (no fix was suggested)
    real_cause: str = ""
    notes: str = ""
    reviewed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def reviews_path(home: Path) -> Path:
    return home / "reviews.jsonl"


def load_reviews(home: Path) -> list[Review]:
    path = reviews_path(home)
    if not path.exists():
        return []
    return [Review(**json.loads(line)) for line in path.read_text().splitlines() if line.strip()]


def pending(home: Path) -> list[Path]:
    """Diagnoses not reviewed yet, oldest first; reused ones (repeats) are skipped."""
    done = {r.diagnosis_file for r in load_reviews(home)}
    out = []
    for path in sorted((home / "diagnoses").glob("*.json"), key=lambda p: p.stat().st_mtime):
        if path.name in done:
            continue
        run = DiagnosisRun.model_validate_json(path.read_text())
        if run.reused_from is None:
            out.append(path)
    return out


def make_review(path: Path, run: DiagnosisRun, answers: dict[str, str]) -> Review:
    return Review(
        diagnosis_file=path.name,
        dag_id=run.job.dag_id,
        task_id=run.job.task_id,
        run_id=run.job.run_id,
        fingerprint=run.fingerprint,
        category=run.diagnosis.category,
        confidence=run.diagnosis.confidence,
        evidence_verified=run.evidence_verified,
        evidence_total=run.evidence_total,
        model=run.served_model or run.model,
        cost_usd=estimate_cost(run),
        latency_s=latency_s(run),
        **answers,
    )


def save_review(home: Path, review: Review) -> None:
    home.mkdir(parents=True, exist_ok=True)
    with reviews_path(home).open("a") as fh:
        fh.write(json.dumps(asdict(review)) + "\n")


# ---- report ---------------------------------------------------------------------------


def _pct(part: int, whole: int) -> str:
    return f"{100 * part / whole:.0f}% ({part}/{whole})" if whole else "-"


def _quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def report(reviews: list[Review]) -> str:
    n = len(reviews)
    if n == 0:
        return "No reviews yet. Run `dagnose review` after some failures were diagnosed."
    cat = Counter(r.category_correct for r in reviews)
    root = Counter(r.root_cause for r in reviews)
    fix = Counter(r.fix for r in reviews)
    graded = [r for r in reviews if r.category_correct != "unsure"]
    lines = [
        "# Dagnose test results",
        "",
        f"Reviewed diagnoses: {n}",
        "",
        "| Measure | Result |",
        "|---|---|",
        f"| Category correct | {_pct(cat['yes'], len(graded))} |",
        f"| Root cause correct | {_pct(root['correct'], n)} |",
        f"| Root cause correct or partly | {_pct(root['correct'] + root['partly'], n)} |",
        f"| Fix useful | {_pct(fix['useful'], n - fix['none'])} |",
        f"| Fix useful or partly | {_pct(fix['useful'] + fix['partly'], n - fix['none'])} |",
    ]
    verified = sum(r.evidence_verified for r in reviews)
    total = sum(r.evidence_total for r in reviews)
    lines.append(f"| Evidence quotes found in the logs | {_pct(verified, total)} |")

    right = [r.confidence for r in reviews if r.root_cause == "correct"]
    wrong = [r.confidence for r in reviews if r.root_cause == "wrong"]
    if right:
        lines.append(f"| Average confidence when right | {statistics.mean(right):.2f} |")
    if wrong:
        lines.append(f"| Average confidence when wrong | {statistics.mean(wrong):.2f} |")
    costs = [r.cost_usd for r in reviews if r.cost_usd is not None]
    if costs:
        lines.append(f"| Cost per diagnosis (average) | USD {statistics.mean(costs):.3f} |")
        lines.append(f"| Cost, all reviewed | USD {sum(costs):.2f} |")
    waits = [r.latency_s for r in reviews if r.latency_s is not None]
    if waits:
        lines.append(f"| Failure to diagnosis, median | {_quantile(waits, 0.5):.0f} s |")
        lines.append(f"| Failure to diagnosis, 90th percentile | {_quantile(waits, 0.9):.0f} s |")

    lines += [
        "",
        "## By category",
        "",
        "| Category | Reviewed | Category correct | Root cause correct |",
        "|---|---|---|---|",
    ]
    by_cat: dict[str, list[Review]] = {}
    for r in reviews:
        by_cat.setdefault(r.category, []).append(r)
    for category, rs in sorted(by_cat.items()):
        g = [r for r in rs if r.category_correct != "unsure"]
        cat_ok = _pct(sum(r.category_correct == "yes" for r in g), len(g))
        root_ok = _pct(sum(r.root_cause == "correct" for r in rs), len(rs))
        lines.append(f"| {category} | {len(rs)} | {cat_ok} | {root_ok} |")

    misses = [r for r in reviews if r.root_cause == "wrong" or r.category_correct == "no"]
    if misses:
        lines += ["", "## Wrong diagnoses", "", "| Task | Said | Real cause |", "|---|---|---|"]
        for r in misses:
            real = r.real_cause.replace("|", "/") or "(not given)"
            lines.append(f"| {r.dag_id}.{r.task_id} | {r.category} ({r.confidence:.2f}) | {real} |")
    return "\n".join(lines) + "\n"


def run_summary(run: DiagnosisRun) -> list[str]:
    """What the reviewer sees before answering."""
    d = run.diagnosis
    out = [
        f"{run.job.dag_id}.{run.job.task_id}   run {run.job.run_id}   try {run.job.try_number}",
        f"Category: {d.category}   confidence {d.confidence:.2f}"
        + ("   (lowered: evidence not found)" if run.confidence_capped else ""),
        "",
        f"Summary:    {d.summary}",
        f"Root cause: {d.root_cause}",
        "Evidence:",
    ]
    for ev in d.evidence:
        where = f"{ev.source}:{ev.line}" if ev.line is not None else ev.source
        out.append(f"  [{'found' if ev.verified else 'not found'}] {where}  {ev.quote[:120]}")
    out.append(f"Fix:        {d.suggested_fix or '(none)'}")
    cost = estimate_cost(run)
    if cost is not None:
        out.append(f"Cost:       USD {cost:.3f}")
    return out


def answers_from(raw: dict[str, Any]) -> dict[str, str]:
    return {k: str(v) for k, v in raw.items()}
