# Testing Dagnose on a real Airflow

This plan takes Dagnose from "works on demo DAGs" to measured results on real failures.
It runs in four phases. Phases 0 to 2 take about a day; phase 3 runs for one to two
weeks alongside normal work.

| Phase | What | Time |
|---|---|---|
| 0. Prepare | Accounts, settings, a place to run the worker | 1 hour |
| 1. Smoke test | One known failure end to end | 30 minutes |
| 2. Scenarios | Fourteen planned cases, including edge cases | half a day |
| 3. Real failures | Every real failure graded with `dagnose review` | 1 to 2 weeks |
| 4. Managed Airflow | MWAA, Cloud Composer, Fabric; models on Bedrock, Vertex AI, Foundry | later |

Results stay on the machine that runs Dagnose (`DAGNOSE_HOME`). They describe your real
failures, so they are never committed to git.

## Phase 0: Prepare

- [ ] **Choose the Airflow.** A development or staging Airflow with real DAGs is ideal.
      Production also works: Dagnose only reads, with a Viewer user.
- [ ] **Create the Viewer user** for Dagnose (see [install.md](install.md), step 1).
- [ ] **Optional, for phase 2 only: a user that can trigger runs** (Op or Admin), used by
      `dev/trigger.py` to start the test DAGs. Dagnose itself never uses it.
- [ ] **Model key** in `DAGNOSE_LLM_API_KEY`, and a **spending limit** with the provider
      (for example USD 10 per month).
- [ ] **A test chat channel** and its webhook: Slack, Microsoft Teams or Google Chat.
- [ ] **Where the worker runs:** next to Airflow in the same Docker Compose project
      ([install.md](install.md)), or on Kubernetes ([deploy-cloud.md](deploy-cloud.md)).
      Give it a persistent volume for `/data`.
- [ ] **Optional: git history.** Mount a clone of the DAG repository and set
      `DAGNOSE_GIT_REPO_PATH` and `DAGNOSE_GIT_DAGS_SUBDIR`.

## Phase 1: Smoke test

1. Run `dagnose check --send-test`. Every line is `[ok]` or `[skip]`, and a test message
   arrives in the chat channel.
2. Start the worker: `docker compose up -d dagnose` (or `dagnose worker`).
3. Make one task fail on purpose. The simplest way is to copy
   `dev/dags/demo/orders_tax.py` into your DAGs folder and trigger `demo_orders_tax`.
4. Within about a minute a card arrives saying "Code bug", pointing at
   `orders_tax.py` line 26, with evidence marked `[found]`.
5. Run `dagnose review` and grade it. The smoke test passes if all of this works.

## Phase 2: Scenarios

Copy `dev/dags/demo/` and `dev/dags/scenarios/` into your DAGs folder (they are tagged
`dagnose-test`; remove them afterwards). Trigger with the UI or:

```bash
uv run python dev/trigger.py scenario_retries scenario_mapped scenario_big_log scenario_killed
```

| # | Scenario | How | Expected |
|---|---|---|---|
| S1 | Code bug | `demo_orders_tax` | Code bug, line 26, fix adds a `country` key |
| S2 | Missing upstream table | `demo_daily_revenue` | Upstream data, table `orders` missing |
| S3 | Timeout | `demo_partner_export` | Timeout, about 5 minutes of work under a 10 second limit |
| S4 | Wrong connection id | `demo_crm_push` | Config, connection `salesforce_prd` does not exist |
| S5 | Missing package | `demo_customer_scoring` | Dependency, `polars_ml_toolkit` not installed |
| S6 | Secrets in logs | `demo_crm_session_sync` | Card and saved files show `[REDACTED:...]` only; see "Checking for leaks" |
| S7 | Retries | `scenario_retries` (2 retries) | One card for the final try (try 3), not one per try |
| S8 | Mapped task | `scenario_mapped` | Only map index 1 is reported; the link opens that instance |
| S9 | Very long log | `scenario_big_log` (20,000 lines) | Exception still found from the last 500 lines; cost stays normal |
| S10 | Killed task (out of memory) | `scenario_killed` | Found by polling; card shows Airflow's "terminated by signal" line |
| S11 | Same failure repeated | `dev/trigger.py demo_crm_push --times 5` | One card and one model call; four diagnoses marked as reused |
| S12 | Burst of different failures | trigger all demo and scenario DAGs at once | All collected; model calls stay under `DAGNOSE_MAX_DIAGNOSES_PER_HOUR` |
| S13 | Worker restart | stop the worker for 5 minutes during failures, start it again | Missed failures are picked up; nothing is reported twice |
| S14 | Chat down | point the webhook at a wrong URL, cause a failure, then fix the URL | Card waits in `dagnose outbox`; `dagnose outbox --resend` delivers it |

Record each scenario as passed or failed, with a note, in your own test log. Grade the
diagnoses of S1 to S12 with `dagnose review`.

## Phase 3: Real failures

Leave the worker running on the real Airflow for one to two weeks. Daily, about five
minutes:

1. `dagnose review`: grade every new diagnosis. Three questions (category, root cause,
   fix) and, when the diagnosis was wrong or partly right, the real cause in a few words.
2. `dagnose queue` and `dagnose outbox`: nothing stuck in `error`, nothing waiting.
3. Note anything surprising: a card that was confusing, a failure Dagnose missed, a
   repeat that was not recognised, a slow card.

Weekly: `dagnose review --report --out week1.md` and read it together.

### Targets

| Measure | Target |
|---|---|
| Real failures reviewed | at least 20 |
| Category correct | 80% or more |
| Root cause correct | 70% or more (correct or partly: 85% or more) |
| Fix useful or partly useful | 70% or more |
| Evidence quotes found in the logs | 85% or more |
| Confidence when wrong | clearly lower than when right |
| Secrets in cards, files or model requests | none |
| Effect on Airflow | none (no slowdown, no errors caused by Dagnose) |
| Cost per diagnosis | USD 0.05 or less on average |
| Failure to diagnosis | 60 seconds or less for 90% of failures (polling every 30 s) |
| Worker | runs the whole period without a manual restart |

## Checking for leaks

Everything Dagnose stores is in `DAGNOSE_HOME`. None of these should find anything:

```bash
grep -rEl 'sk-ant-|AKIA[0-9A-Z]{16}|BEGIN [A-Z ]*PRIVATE KEY|xox[abposr]-' "$DAGNOSE_HOME"
grep -rl 'A_SECRET_VALUE_YOU_KNOW_IS_IN_YOUR_LOGS' "$DAGNOSE_HOME"
```

For S6 use the planted values listed under `must_not_leak` in
`dev/dags/demo/expected.json`. If anything is found, stop the test and report it
privately as described in [SECURITY.md](../SECURITY.md).

## Phase 4: Managed Airflow and cloud models (later)

The same plan on each managed platform, after self-hosted testing passes:

- **AWS:** MWAA with `DAGNOSE_AIRFLOW_AUTH=mwaa`, and Claude on Bedrock.
- **Google Cloud:** Cloud Composer with `DAGNOSE_AIRFLOW_AUTH=google`, and Claude on
  Vertex AI.
- **Azure:** Airflow in Microsoft Fabric with `DAGNOSE_AIRFLOW_AUTH=azure`, and Claude on
  Foundry.

Phases 1 and 2 are enough on each, plus a check that the IAM or role setup in
[install.md](install.md#managed-airflow-aws-google-cloud-azure) is complete. Create the
managed environment for the test and delete it the same day to keep costs low.

## Reporting results

Share the weekly report (`dagnose review --report`) and the scenario log. The report
contains DAG names and the real causes you typed, so share it only with people who may
see those. Problems with Dagnose itself go to GitHub issues, without logs or secrets.
