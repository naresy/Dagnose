# Contributing to Dagnose

Thank you for helping. This guide covers the development setup, the tests, and how a
change gets merged.

## Development setup

Requirements: Python 3.10 or newer, [uv](https://docs.astral.sh/uv/), Docker with at
least 4 GB of memory, and git.

```bash
git clone https://github.com/naresy/dagnose.git
cd dagnose
uv sync                      # installs the package and the development tools
./dev/install_hooks.sh       # pre-commit hook that blocks commits containing keys
```

Run the checks that CI runs:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
```

The unit tests replay recorded Airflow API traffic (`tests/fixtures/api/`), so they need
no running Airflow and no API key. Tests never call a real model.

## Local Airflow for development

Two Airflow versions run side by side with the same demo DAGs and the listener plugin
loaded from `src/`:

| Airflow | Start | UI and API | Login |
|---|---|---|---|
| 3.3 | `cd dev && docker compose up -d` | http://localhost:8080 | airflow / airflow |
| 2.11 | `cd dev/airflow2 && docker compose up -d` | http://localhost:8081 | airflow / airflow |

`dev/dags/demo/` holds DAGs that fail on purpose, and `dev/dags/scenarios/` holds edge cases
(retries, a mapped task, a very long log, a killed task) used by the [testing plan](docs/testing.md). They are written like ordinary
pipelines, so their names and comments do not give the cause away; what each one does
wrong is in [dev/dags/demo/README.md](dev/dags/demo/README.md). One of them leaks fake
credentials to test redaction. What each DAG must produce is in
`dev/dags/demo/expected.json`, shared by the unit tests and the end-to-end check.

Run the worker against the local Airflow 3:

```bash
export DAGNOSE_HOME=dev/dagnose-data
export DAGNOSE_AIRFLOW_USERNAME=airflow DAGNOSE_AIRFLOW_PASSWORD=airflow
uv run dagnose check
uv run dagnose worker --once
```

For Airflow 2, set `DAGNOSE_AIRFLOW_URL=http://localhost:8081` and
`DAGNOSE_HOME=dev/airflow2/dagnose-data`.

## End-to-end checks

`dev/e2e.py` triggers every demo DAG on a running Airflow, waits for the failures,
runs the worker, and checks the collected and redacted context and the chat card
(against a local fake webhook) for each one:

```bash
uv run python dev/e2e.py --airflow 3                # listener plugin and polling
uv run python dev/e2e.py --airflow 3 --watch api    # polling only
uv run python dev/e2e.py --airflow 2
```

## Other development scripts

| Script | Purpose |
|---|---|
| `dev/record_fixtures.py` | Record API traffic and contexts from a running Airflow into `tests/fixtures/` |
| `dev/diagnose_fixtures.py --yes-spend` | Score the real model on the recorded failures. Calls the model API and costs money |
| `dev/trigger.py DAG_ID [--times N]` | Trigger DAG runs on any Airflow 2 or 3 (needs a user that may trigger) |
| `dev/scan_secrets.py` | Fail if a real-looking key is in the files git would commit (also runs in CI) |

## Project layout

| Path | Contents |
|---|---|
| `src/dagnose/plugin/` | Airflow listener plugin: writes failure jobs to the spool, never raises |
| `src/dagnose/queue.py` | Spool (listener side) and SQLite job queue (worker side) |
| `src/dagnose/watcher.py` | Finds failed tasks by polling the Airflow REST API |
| `src/dagnose/collector/` | Airflow REST API client (v1 and v2), cloud logins, logs, git history |
| `src/dagnose/redactor/` | Secret detection and redaction |
| `src/dagnose/incidents.py` | Failure fingerprints, repeats and the hourly model limit |
| `src/dagnose/agent/` | Diagnosis loop, read-only tools, evidence checks, model providers |
| `src/dagnose/notifier/` | Cards for Slack, Microsoft Teams, Google Chat and email; outbox |
| `src/dagnose/check.py` | `dagnose check` |
| `src/dagnose/worker.py` | The pipeline run for each failure |
| `deploy/` | Kubernetes manifest and ECS task definition |
| `docs/` | Installation and deployment guides |

## Continuous integration

| When | Workflow | What runs |
|---|---|---|
| Every pull request and push to `main` | `.github/workflows/ci.yml` | Lint and strict type checks; unit tests on Python 3.10 to 3.13 with a coverage gate; end-to-end on real Airflow 3 and 2 in Docker, with the plugin and with polling only; package and container image builds |
| A tag `vX.Y.Z` | `.github/workflows/release.yml` | All of the above, then publish to PyPI and the container image to ghcr.io, and create a GitHub release |
| Weekly | `.github/dependabot.yml` | Grouped dependency updates |

## Pull requests

- Open pull requests against `main`. One topic per pull request.
- Add or update tests for the behaviour you change. Bugs get a regression test.
- Keep everything that leaves the process redacted: chat cards, emails, model requests
  and files written to disk.
- Do not commit keys or real credentials. Test tokens are assembled at runtime so that
  no key-shaped string sits in the repository.
- By contributing you agree that your contribution is licensed under the Apache
  License 2.0.

## Reporting security issues

Please do not open a public issue. See [SECURITY.md](SECURITY.md).
