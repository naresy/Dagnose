# Dagnose

[![CI](https://github.com/naresy/dagnose/actions/workflows/ci.yml/badge.svg)](https://github.com/naresy/dagnose/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](pyproject.toml)

**An open source on-call assistant for Apache Airflow.** When a task fails, Dagnose
collects the evidence, removes secrets, asks an LLM what went wrong, checks the answer
against the real logs and code, and posts a short diagnosis to Slack, Microsoft Teams,
Google Chat or email.

Dagnose (DAG + diagnose) is self-hosted, read-only towards Airflow, and uses your own
LLM account.

## Contents

- [What you get](#what-you-get)
- [How it works](#how-it-works)
- [Supported environments](#supported-environments)
- [Quick start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Commands](#commands)
- [Security and privacy](#security-and-privacy)
- [Costs](#costs)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## What you get

A failed task produces a card like this, a few seconds after Airflow marks it failed:

```text
demo_orders_tax.compute_tax failed
Code bug - confidence 0.93

compute_tax fails because the order produced by extract_orders has no 'country'
field, but compute_tax looks up the tax rate by order['country'].

Evidence (checked against the logs and code)
  [found] dag_source:26  rate = TAX_RATES[order["country"]]
  [found] dag_source:22  return {"order_id": 42, "amount": 19.99, "currency": "USD"}
  [found] exception      KeyError: 'country'

Suggested fix
  Add a 'country' key to the dict returned by extract_orders.

Run manual__2026-11-23T16:53  |  Try 1  |  Airflow 3.3.2         [Open in Airflow]
```

- **A diagnosis, not just an alert:** category, root cause, suggested fix and, for code
  bugs, a patch.
- **Evidence you can check:** every quote is looked up in the real logs and DAG code.
  When quotes cannot be found, the confidence is lowered.
- **Secrets removed first:** passwords, tokens, keys, emails and names are replaced
  before anything leaves your network or is written to disk.
- **Quiet on repeats:** the same failure again reuses its diagnosis and gets at most
  one "still failing" update per hour.
- **Works without a model:** without an API key, failures are still collected and
  announced, marked "not diagnosed".

## How it works

```text
  Airflow                         dagnose worker (one container)
  -------                         ------------------------------
  task fails  --REST API poll-->  1. collect   log, exception, DAG source, inputs,
  (or listener plugin)                         run history, git commits
                                  2. redact    replace secrets with placeholders
                                  3. dedupe    same failure seen before? reuse it
                                  4. diagnose  LLM with read-only tools
                                  5. verify    check every evidence quote
                                  6. notify    Slack / Teams / Google Chat / email
```

- **Finding failures:** by default the worker polls the Airflow REST API every 15
  seconds for failed tasks. No plugin, no changes to your Airflow image or DAGs, and it
  works with Kubernetes and Celery executors, managed Airflow, and tasks killed from
  outside (for example out of memory). An optional listener plugin reports failures
  instantly.
- **Separate from Airflow:** the worker is its own process, so a slow LLM call or a
  bug in Dagnose can never slow down or break Airflow.
- **Read-only:** Dagnose reads DAGs, runs, task instances and logs with an Airflow
  user that has the Viewer role. It never changes anything in Airflow.

## Supported environments

| Area | Supported | Status |
|---|---|---|
| Airflow | 3.x (REST API v2), 2.x (REST API v1) | Tested end to end on 3.3.2 and 2.11.1 |
| Self-hosted Airflow | Docker Compose, Kubernetes (EKS, GKE, AKS), VMs | Tested with Docker Compose |
| AWS | Amazon MWAA (IAM, `InvokeRestApi`) | Built and tested offline, not yet run on MWAA |
| Google Cloud | Cloud Composer (Google credentials) | Built and tested offline, not yet run on Composer |
| Azure | Airflow on AKS; Airflow in Microsoft Fabric (Entra ID) | AKS as self-hosted; Fabric experimental |
| Astronomer | Deployment API token | Expected to work, not yet verified |
| LLM | Claude API, Amazon Bedrock, Google Vertex AI, Microsoft Foundry | Claude API tested live; others tested offline |
| Notifications | Slack, Microsoft Teams, Google Chat, email (SMTP) | Tested offline and with a local webhook |

## Quick start

The shortest path for an Airflow you run with Docker Compose. The full guide, including
Kubernetes and managed Airflow, is in [docs/install.md](docs/install.md).

1. **Create a read-only Airflow user** (Airflow 3 with the FAB auth manager, or Airflow 2):

   ```bash
   docker compose exec airflow-apiserver airflow users create \
     --role Viewer --username dagnose --password 'choose-a-long-password' \
     --firstname Dagnose --lastname Bot --email dagnose@example.com
   ```

2. **Create `dagnose.env`** next to your `docker-compose.yml` and keep it out of git:

   ```bash
   DAGNOSE_AIRFLOW_USERNAME=dagnose
   DAGNOSE_AIRFLOW_PASSWORD=choose-a-long-password
   DAGNOSE_LLM_API_KEY=sk-ant-...
   DAGNOSE_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
   ```

3. **Add the worker** to your `docker-compose.yml`:

   ```yaml
   services:
     dagnose:
       image: ghcr.io/naresy/dagnose:latest
       restart: unless-stopped
       env_file: dagnose.env
       environment:
         DAGNOSE_AIRFLOW_URL: http://airflow-apiserver:8080   # Airflow 2: http://airflow-webserver:8080
         DAGNOSE_WATCH: api
       volumes:
         - dagnose-data:/data

   volumes:
     dagnose-data:
   ```

4. **Check the setup, then start it:**

   ```bash
   docker compose run --rm dagnose check
   docker compose up -d dagnose
   ```

   `check` verifies the Airflow connection and login, how failures are found, task log
   access, the LLM key (free, no tokens used) and the notification channels, and tells
   you what to change if something is wrong.

## Installation

| Method | Command | Use it for |
|---|---|---|
| Docker image | `docker pull ghcr.io/naresy/dagnose:latest` | Production. Includes the AWS, Google Cloud and Azure SDKs. Intel and ARM. |
| pip | `pip install dagnose` | Running on a host, or installing the listener plugin into your Airflow image. |
| pip with cloud extras | `pip install 'dagnose[aws]'`, `[gcp]`, `[azure]` | MWAA, Composer, Entra ID, Bedrock, Vertex AI, Foundry outside the Docker image. |
| From source | `git clone` then `uv sync` | Development, see [CONTRIBUTING.md](CONTRIBUTING.md). |

The PyPI package and the container image are published from release v0.1.0 onwards.

Deployment guides: [Docker Compose and managed Airflow](docs/install.md),
[Kubernetes, ECS, Cloud Run, Container Apps](docs/deploy-cloud.md).
To evaluate Dagnose on your own Airflow, follow the [testing plan](docs/testing.md).

## Configuration

All settings are environment variables starting with `DAGNOSE_`. They can also go in a
`.env` file in the working directory; real environment variables take precedence.
The most important ones:

| Setting | Default | Purpose |
|---|---|---|
| `DAGNOSE_AIRFLOW_URL` | `http://localhost:8080` | Airflow API server (Airflow 2: webserver) |
| `DAGNOSE_AIRFLOW_USERNAME`, `_PASSWORD` | none | Read-only Airflow user (Viewer role) |
| `DAGNOSE_AIRFLOW_TOKEN` | none | Bearer token instead of a username and password |
| `DAGNOSE_AIRFLOW_AUTH` | `password` | `password`, `mwaa`, `google` (Composer) or `azure` (Entra ID) |
| `DAGNOSE_WATCH` | `both` | How failures are found: `api` (polling), `plugin` or `both` |
| `DAGNOSE_WATCH_DAG_IDS` | all DAGs | JSON list, to watch only some DAGs |
| `DAGNOSE_LLM_PROVIDER` | `anthropic` | `anthropic`, `bedrock`, `vertex` or `foundry` |
| `DAGNOSE_LLM_MODEL` | `claude-opus-5-5` | Model name |
| `DAGNOSE_LLM_API_KEY` | none | Claude API or Foundry key |
| `DAGNOSE_DIAGNOSE` | `true` | `false` collects and notifies without calling a model |
| `DAGNOSE_MAX_DIAGNOSES_PER_HOUR` | `20` | Upper bound on model calls (`0` for no limit) |
| `DAGNOSE_DEDUPE_WINDOW_HOURS` | `24` | Reuse the diagnosis of a repeated failure for this long |
| `DAGNOSE_RENOTIFY_MINUTES` | `60` | Minimum time between "still failing" updates |
| `DAGNOSE_SLACK_WEBHOOK_URL` | none | Slack incoming webhook |
| `DAGNOSE_TEAMS_WEBHOOK_URL` | none | Microsoft Teams Workflows webhook |
| `DAGNOSE_GOOGLE_CHAT_WEBHOOK_URL` | none | Google Chat space webhook |
| `DAGNOSE_SMTP_HOST`, `_EMAIL_FROM`, `_EMAIL_TO` | none | Email; `_EMAIL_TO` is a JSON list |
| `DAGNOSE_GIT_REPO_PATH`, `_GIT_DAGS_SUBDIR` | none | A clone of your DAG repository, for recent commits |
| `DAGNOSE_HOME` | `~/.dagnose` | Where the queue and results are stored |

Cloud-specific settings (MWAA environment, Vertex project, Foundry resource and others)
are described in [docs/install.md](docs/install.md#managed-airflow-aws-google-cloud-azure).

## Commands

| Command | What it does |
|---|---|
| `dagnose check [--send-test]` | Verify the whole setup; `--send-test` posts a test message |
| `dagnose worker [--once]` | Run the pipeline continuously (or until the queue is empty) |
| `dagnose queue` | Show job counts by status |
| `dagnose collect DAG_ID RUN_ID TASK_ID` | Collect one failure and print it as redacted JSON |
| `dagnose redact FILE` | Redact a saved failure context |
| `dagnose diagnose FILE` | Diagnose a saved failure context (calls the model) |
| `dagnose notify FILE [--dry-run]` | Send or preview the card for a saved failure |
| `dagnose outbox [--resend]` | Notifications that could not be delivered |
| `dagnose review [--report]` | Grade diagnoses by hand, then report accuracy, cost and speed |
| `dagnose version` | Print the version |

## Security and privacy

- **Secrets are removed before anything leaves the process.** Passwords in connection
  strings, `password=`, `token=` and `api_key=` values, known token formats (AWS,
  GitHub, Slack, Stripe, Google, OpenAI, Anthropic, JWT, private keys), email addresses,
  random-looking keys, sensitive task parameters and git author names are replaced with
  placeholders such as `[REDACTED:password#1]`. The same value always gets the same
  placeholder, and the original values are never stored.
- **Only redacted data is written to disk or sent** to the model and to chat or email.
  Cards contain short quotes, never full logs, and are redacted a second time.
- **Read-only towards Airflow.** A Viewer user is enough.
- **Your own model account.** Prompts go to the Claude API or, if you prefer, to Claude
  inside your own AWS, Google Cloud or Azure account.
- To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Costs

Dagnose itself is free. The model is billed by your provider: a diagnosis with Claude
Opus 5.5 costs about USD 0.03. Repeated failures reuse their diagnosis, and
`DAGNOSE_MAX_DIAGNOSES_PER_HOUR` caps model calls. Setting a spending limit with your
model provider is still recommended.

## Roadmap

Available now:

- Failure detection by REST API polling or listener plugin, for Airflow 2 and 3
- Collection, redaction, repeat detection, diagnosis with evidence checks
- Slack, Microsoft Teams, Google Chat and email
- Managed Airflow logins and the model inside AWS, Google Cloud and Azure
- `dagnose check`, Docker image, Kubernetes and ECS deployment files

Planned:

- **Impact:** which downstream DAGs and deadlines a failure affects
- **Retry or fix:** whether a failure is safe to retry
- **Prompt audit log** and a dry run that shows exactly what would be sent to the model
- **Routing** of cards by DAG owner or tag
- **Incident memory and feedback:** "seen this before, fixed by ...", correct or wrong buttons
- **Fix pull requests** for code bugs, always approved by a person
- **Evaluation set** of real failure cases to track diagnosis quality

## Contributing

Contributions are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) explains the development
setup with local Airflow 2 and 3, the test suite and the end-to-end checks.

## License

Apache License 2.0, see [LICENSE](LICENSE). Release notes are in
[CHANGELOG.md](CHANGELOG.md).

Apache Airflow, Airflow and the Airflow logo are trademarks of the Apache Software
Foundation. Dagnose is an independent project and is not affiliated with or endorsed by
the Apache Software Foundation.
