"""Run the real model on the recorded failures and score the diagnoses. COSTS MONEY.

Diagnoses each recorded FailureContext in tests/fixtures/contexts/<api>/ (redacted
first, like the worker does) and compares the category with
dev/dags/demo/expected.json. Tools are off (no live Airflow needed), so this
measures the diagnosis from the collected context alone.

    export ANTHROPIC_API_KEY=...            # or AIRFLOW_DOCTOR_LLM_API_KEY, or `ant auth login`
    uv run python dev/diagnose_fixtures.py --yes-spend            # 6 diagnoses, Airflow 3
    uv run python dev/diagnose_fixtures.py --yes-spend --api v1   # Airflow 2 recordings
    uv run python dev/diagnose_fixtures.py --yes-spend --only demo_orders_tax

Each diagnosis is saved to dev/doctor-data/eval/<api>/<dag_id>.json for review.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from airflow_doctor.agent import DiagnosisError, ProviderError, ToolBox, diagnose, make_provider
from airflow_doctor.config import Settings
from airflow_doctor.models import FailureContext
from airflow_doctor.redactor import Redactor

ROOT = Path(__file__).resolve().parent.parent
EXPECTED = {
    k: v
    for k, v in json.loads((ROOT / "dev/dags/demo/expected.json").read_text()).items()
    if not k.startswith("_")
}
# Opus 5.5 list prices per 1M tokens (input, output, cache read). Estimate only.
PRICES = {"claude-opus-5-5": (4.00, 20.00, 0.20)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yes-spend", action="store_true", help="confirm paid model calls")
    parser.add_argument("--api", choices=["v1", "v2"], default="v2")
    parser.add_argument("--only", help="one dag_id")
    args = parser.parse_args()
    if not args.yes_spend:
        print("This calls the model API and costs money. Re-run with --yes-spend.")
        return 2

    settings = Settings()
    provider = make_provider(settings)
    out_dir = ROOT / "dev/doctor-data/eval" / args.api
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = sorted((ROOT / "tests/fixtures/contexts" / args.api).glob("*.json"))
    if args.only:
        paths = [p for p in paths if p.stem == args.only]

    correct = 0
    cost = 0.0
    for path in paths:
        accepted = EXPECTED[path.stem]["category"]
        accepted = accepted if isinstance(accepted, list) else [accepted]
        session = Redactor.from_settings(settings).session()
        ctx = session.redact(FailureContext.model_validate_json(path.read_text()))
        try:
            run = diagnose(
                ctx, provider, ToolBox(ctx, None, session), max_turns=settings.llm_max_turns
            )
        except (ProviderError, DiagnosisError) as exc:
            print(f"ERROR {path.stem}: {exc}")
            continue
        (out_dir / path.name).write_text(run.model_dump_json(indent=2))
        d = run.diagnosis
        ok = d.category in accepted
        correct += ok
        price = PRICES.get(provider.model)
        if price:
            u = run.usage
            cost += (
                u.input_tokens * price[0]
                + u.output_tokens * price[1]
                + u.cache_read_input_tokens * price[2]
                + u.cache_creation_input_tokens * price[0] * 1.25  # 5-minute cache writes
            ) / 1e6
        print(
            f"{'ok  ' if ok else 'MISS'} {path.stem:24} {d.category:14} "
            f"(want {'/'.join(accepted):24}) "
            f"conf {d.confidence:.2f} evidence {run.evidence_verified}/{run.evidence_total}"
        )
        print(f"     {d.summary}")
    print(f"\n{correct}/{len(paths)} categories correct; estimated cost ${cost:.3f}")
    return 0 if correct == len(paths) else 1


if __name__ == "__main__":
    sys.exit(main())
