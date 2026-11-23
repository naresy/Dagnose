# Airflow Doctor

[![ci](https://github.com/naresy/airflow_doctor/actions/workflows/ci.yml/badge.svg)](https://github.com/naresy/airflow_doctor/actions/workflows/ci.yml)

Open source AI on-call engineer for Apache Airflow. It diagnoses failed tasks with evidence, suggests fixes as pull requests, and remembers past incidents. Self-hosted, and you bring your own LLM key.

## Why

When an Airflow task fails, engineers lose hours digging through logs, DAG code and run history to find out why. Airflow Doctor does that work for you, inside your own Airflow. Your data never leaves your network.

## How it works

1. **Trigger:** an Airflow plugin listener (`on_task_instance_failed`) writes a tiny job file (`dag_id`, `run_id`, `task_id`, `try_number`) into a spool folder with an atomic rename, and returns. No locks, so many task processes can fail at once safely, even over Docker bind mounts or NFS. It never raises and never calls an LLM, so Airflow is never slowed down or crashed by Doctor.
2. **Collect:** a separate `airflow-doctor worker` process moves spool files into its own SQLite queue (only the worker touches it), then reads everything through the Airflow REST API (`/api/v2` on 3.x, `/api/v1` on 2.x). It gathers the log tail, the exception and stack trace, the DAG source, task params, the last N runs, and recent git commits that touched the DAG.
3. **Redact:** it replaces secrets with placeholders like `[REDACTED:password#1]` before anything reaches a model or disk: passwords in connection strings, `password=`/`token=`/`api_key=` values, known token formats (AWS, GitHub, Slack, Stripe, Google, OpenAI, Anthropic, JWT, private keys), emails, unlabelled high-entropy keys, sensitive task params and git author names. The same value always gets the same placeholder, so the model can still see that one secret appears in two places. Redaction is one-way: originals are never stored.
4. **Recall:** it searches local incident memory for similar past failures, using an error fingerprint plus embeddings.
5. **Diagnose:** an LLM gets the redacted context with numbered log and DAG source lines, plus read-only tools (`list_run_tasks`, `get_task_log` for upstream tasks or the last successful run; `get_table_schema` and `search_past_incidents` come later). It returns structured JSON: summary, category, root cause, evidence quotes, confidence, fix and an optional patch. Every evidence quote is checked against the context, and confidence is capped when the evidence does not check out. Tool results are redacted with the same placeholders before the model sees them.
6. **Fix:** if a code fix applies, it opens a pull request through the GitHub or GitLab API. It never touches production.
7. **Notify:** it posts a Slack and/or email card with the diagnosis, checked evidence (✅ found in the logs / ❔ not found), the suggested fix and an "Open in Airflow" link. Cards carry only redacted text and short quotes, never raw logs, and are redacted once more before sending. Without a diagnosis the card still shows the error, where it failed and why it was not diagnosed. Failed deliveries go to an outbox (`airflow-doctor outbox --resend`) instead of re-running the pipeline. PR links and correct / wrong / retry buttons come with steps 5 and 6.
8. **Learn:** feedback and final fixes are saved to memory, so later diagnoses improve.

```
Airflow (scheduler / task process)          airflow-doctor worker (separate process)
┌────────────────────────────┐  job (tiny)  ┌───────────────────────────────────────────┐
│ listener: task failed      │ ───────────► │ collector ──► redactor ──► memory (recall) │
│ → spool file, return,      │ spool folder │      ──► agent (LLM + read-only tools)    │
│   never raise              │              │      ──► fixer (PR) / notifier / memory   │
└────────────────────────────┘              └───────────────────────────────────────────┘
                 ▲                                         │
                 └──────────── Airflow REST API ◄──────────┘
```

Why a separate worker: Airflow 3 tasks cannot touch the metadata DB directly, the task process exits right after failing, and the REST API works the same for 2.x and 3.x (including remote logging). Replaying a failure is just putting a job in the same queue.

## Components

| Module | Role |
|---|---|
| `plugin` | Airflow listener plugin: write failure jobs to the spool only |
| `queue` | Lock-free spool (listener side) + SQLite job queue (worker side) |
| `worker` | Separate process that runs the pipeline for each job |
| `collector` | REST API client (v2 / v1), log and metadata readers, git history |
| `redactor` | Secret scrubbing: token formats, credential URLs, sensitive keys, emails, entropy, people |
| `memory` | SQLite + sqlite-vec incident store, error fingerprints, embeddings |
| `agent` | Diagnosis loop, read-only tools, evidence check; provider interface with an Anthropic adapter |
| `fixer` | Branch and PR creation (GitHub / GitLab) |
| `notifier` | Slack (Block Kit, incoming webhook) and email (SMTP) cards, retries, outbox |
| `cli` | `airflow-doctor` commands: setup, test, replay a failure |

## Tech stack

- **Core:** Python 3.10+, `uv`, Apache Airflow 2.x/3.x plugin and listener API
- **LLM:** official Anthropic SDK (Claude Opus 5.5 by default: structured outputs, strict tools, prompt caching, refusal fallback) behind a small provider interface; OpenAI and local (Ollama) adapters can be added without touching the agent. Pydantic for the diagnosis schema
- **Security:** own redaction detectors (known token formats, credential URLs, sensitive keys, emails, entropy), tuned to keep diagnostic evidence such as hosts, ids and class names
- **Memory:** SQLite with `sqlite-vec`, `fastembed` for local embeddings
- **Integrations:** PyGithub, python-gitlab, Slack SDK, SMTP
- **CLI:** Typer
- **Quality:** pytest, ruff, mypy, GitHub Actions
- **Eval:** Docker Compose with a local Airflow and deliberately broken DAGs (30+ real failure cases)

## Install (planned)

```bash
pip install airflow-doctor
export AIRFLOW_DOCTOR_LLM_MODEL="claude-opus-5-5"   # default; other providers planned
export AIRFLOW_DOCTOR_LLM_API_KEY="..."               # or ANTHROPIC_API_KEY, or `ant auth login`
export AIRFLOW_DOCTOR_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."
# and/or email:
# export AIRFLOW_DOCTOR_SMTP_HOST=smtp.example.com AIRFLOW_DOCTOR_EMAIL_FROM=doctor@example.com
# export AIRFLOW_DOCTOR_EMAIL_TO='["oncall@example.com"]'   # JSON list; also SMTP_PORT/_USERNAME/_PASSWORD/_SECURITY
airflow-doctor setup
```

Settings can also go in a `.env` file in the directory you run `airflow-doctor` from (git-ignored; real environment variables win over it):

```bash
# .env
AIRFLOW_DOCTOR_LLM_API_KEY=sk-ant-...
AIRFLOW_DOCTOR_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

## Development

```bash
uv sync                                   # install
uv run ruff check . && uv run mypy && uv run pytest

cd dev && docker compose up -d            # local Airflow 3.3 at http://localhost:8080 (airflow / airflow)
```

The dev stack mounts `src/` into Airflow and loads the listener from `dev/plugins/`. Failed tasks land in `dev/doctor-data/spool/`.

`dev/dags/broken/` holds deliberately broken DAGs (code bug, missing table, timeout, bad connection, missing dependency, and one that leaks fake credentials to test redaction). Trigger them from the UI or REST API to produce real failures, then:

```bash
export AIRFLOW_DOCTOR_HOME=dev/doctor-data
export AIRFLOW_DOCTOR_AIRFLOW_USERNAME=airflow AIRFLOW_DOCTOR_AIRFLOW_PASSWORD=airflow
export AIRFLOW_DOCTOR_GIT_REPO_PATH=$PWD AIRFLOW_DOCTOR_GIT_DAGS_SUBDIR=dev/dags   # optional

uv run airflow-doctor worker --once       # spool -> queue -> collect -> dev/doctor-data/contexts/*.json
uv run airflow-doctor queue               # job counts by status
uv run airflow-doctor collect <dag_id> <run_id> <task_id>   # one failure, redacted JSON to stdout (--raw: unredacted)
uv run airflow-doctor redact <context.json>                 # redact a saved context, summary on stderr
uv run airflow-doctor diagnose <context.json>               # diagnose a saved context (calls the model API)
uv run python dev/diagnose_fixtures.py --yes-spend          # score the model on the 6 recorded failures (paid)
uv run airflow-doctor notify <context.json> --dry-run       # preview the Slack and email card
uv run airflow-doctor outbox [--resend]                     # notifications that could not be delivered
uv run python dev/record_fixtures.py      # refresh recorded API traffic + contexts in tests/fixtures/
```

Tests replay recorded real Airflow API traffic (`tests/fixtures/api/`), so they need no running Airflow. `tests/fixtures/contexts/` holds the collected `FailureContext` for each broken DAG, the input for the redactor and agent steps.

### Airflow 2.x

Airflow support: 3.x first, 2.x kept working through the v1 REST API. Both are tested against real installs (3.3.2 and 2.11.1). A 2.x stack runs next to the 3.x one, with the same broken DAGs and plugin:

```bash
cd dev/airflow2 && docker compose up -d   # Airflow 2.11 at http://localhost:8081 (airflow / airflow)

export AIRFLOW_DOCTOR_HOME=dev/airflow2/doctor-data AIRFLOW_DOCTOR_AIRFLOW_URL=http://localhost:8081
uv run airflow-doctor worker --once       # same commands as above
AIRFLOW_DOCTOR_AIRFLOW_URL=http://localhost:8081 uv run python dev/record_fixtures.py   # v1 fixtures
```

### CI/CD

| When | Workflow | What runs |
|---|---|---|
| Every PR and push to `main` | [ci.yml](.github/workflows/ci.yml) | ruff + strict mypy; unit tests on Python 3.10–3.13 (coverage ≥ 85%); **e2e on real Airflow 3 and Airflow 2** in Docker (`dev/e2e.py`: fail the broken DAGs, check listener → worker → collect → redact → Slack card against a local fake webhook); build the wheel and smoke test it in a clean venv |
| Push a tag `vX.Y.Z` | [release.yml](.github/workflows/release.yml) | everything above, then check the tag matches `pyproject.toml`, publish to PyPI (trusted publishing), create a GitHub release |
| Weekly | [dependabot.yml](.github/dependabot.yml) | grouped update PRs for Python deps and Actions |

Run the e2e check locally the same way CI does: start a stack, then `uv run python dev/e2e.py --airflow 3` (or `--airflow 2`). What each broken DAG must produce lives in `dev/dags/broken/expected.json`, shared by the unit tests and the e2e check.

## Roadmap

We build one piece at a time, and each piece is tested before the next one starts.

**Status:** steps 1, 2 and 4 done, plus CI/CD. Step 3 is built and tested offline; its first live run needs an API key. **Next: step 5, incident memory.**

- [x] **0. Foundation:** project scaffold, local Airflow 3 and 2 in Docker, 5 broken DAGs, CI/CD pipeline (lint, types, tests on Python 3.10–3.13, e2e on real Airflow 2 and 3, build, release to PyPI)
- [x] **1. Collector:** capture real failure data from a local Airflow (Docker Compose + broken DAGs)
  - listener plugin, lock-free spool, worker queue with retry backoff
  - REST API client for Airflow 3 (`/api/v2`) and Airflow 2 (`/api/v1`), verified on 3.3.2 and 2.11.1
  - collects exception and failing line, log tail, DAG source, task params, run history, git commits
- [x] **2. Redactor:** secret scrubbing with tests
  - placeholders keep evidence (hosts, users, ids, failing lines); the worker writes only redacted data
  - checked on real Airflow 2 and 3: planted fake secrets never reach the saved context, and clean failures lose nothing but author names
- [ ] **3. Diagnosis agent:** LLM + read-only tools, structured JSON output ← **built, tested offline; live run pending an API key**
  - Claude via the Anthropic SDK behind a provider interface; strict tools, structured output, refusal fallback, prompt caching
  - evidence quotes checked against the context; confidence capped when they do not check out
  - without credentials the worker still collects and redacts, and skips the diagnosis
- [x] **4. Slack notifier:** diagnosis card with evidence
  - Slack (incoming webhook) and email (SMTP); redacted again before sending; retries, then an outbox
  - e2e on real Airflow 2 and 3: one card per failure, no planted secret in any card
  - feedback buttons (correct / wrong / retry) need a Slack app endpoint: step 5
- [ ] **5. Incident memory:** SQLite + embeddings, "seen this before" ← **next**
- [ ] **6. Fix PRs:** GitHub / GitLab pull requests
- [ ] **7. Eval set:** 30 failure cases, accuracy tracking
- [ ] **8. Launch:** docs, 60-second demo video, Hacker News, r/dataengineering, Airflow Slack, LinkedIn

Planned before launch: a mode that needs no plugin (the worker polls the Airflow API for failed tasks). It is needed for Kubernetes/Celery workers, managed Airflow (MWAA, Composer), and tasks killed for running out of memory.


## Principles

- Advisory and read-only by default. Humans approve every fix.
- No data leaves the customer's network, apart from redacted context sent to their own chosen LLM.
- Give evidence instead of plausible guesses. Show the log lines that prove the diagnosis.

## License

Apache 2.0 (planned)
