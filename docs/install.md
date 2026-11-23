# Install Airflow Doctor in your Airflow project

Airflow Doctor runs **next to** your Airflow as one extra container. In the default
setup it only needs a read-only Airflow login: no plugin, no changes to your DAGs or
your Airflow image, no shared folder. It finds failed tasks by polling Airflow's REST
API, then collects, redacts, diagnoses and sends a card to Slack or email.

Tested with **Airflow 3.3** and **Airflow 2.11** (Docker Compose, LocalExecutor).
Time: about 10 minutes.

## 1. Create a read-only Airflow user

Doctor only reads: DAGs, DAG runs, task instances and logs. The built-in **Viewer**
role is enough.

**Airflow 3 with the FAB auth manager, or Airflow 2** (in your Airflow container):

```bash
docker compose exec airflow-apiserver airflow users create \
  --role Viewer --username doctor --password 'choose-a-long-password' \
  --firstname Airflow --lastname Doctor --email doctor@example.com
```

On Airflow 2 the container is usually `airflow-webserver`, and the REST API must accept
basic auth: `AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session`.

**Airflow 3 with the default simple auth manager:** add `doctor:viewer` to
`[core] simple_auth_manager_users` (`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`); the
password is written to the generated passwords file.

## 2. Settings: `doctor.env`

Next to your `docker-compose.yml`, create `doctor.env` (keep it out of git):

```bash
AIRFLOW_DOCTOR_AIRFLOW_USERNAME=doctor
AIRFLOW_DOCTOR_AIRFLOW_PASSWORD=choose-a-long-password
AIRFLOW_DOCTOR_LLM_API_KEY=sk-ant-...            # Anthropic key for the diagnosis
# No key yet? Use AIRFLOW_DOCTOR_DIAGNOSE=false instead: failures are still collected,
# redacted and sent, and cards say "Not diagnosed".
AIRFLOW_DOCTOR_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...   # optional
```

```bash
chmod 600 doctor.env && echo doctor.env >> .gitignore
```

## 3. Add the worker to your `docker-compose.yml`

```yaml
services:
  # ... your Airflow services ...

  airflow-doctor:
    image: ghcr.io/naresy/airflow-doctor:latest
    restart: unless-stopped
    env_file: doctor.env
    environment:
      AIRFLOW_DOCTOR_AIRFLOW_URL: http://airflow-apiserver:8080   # Airflow 2: http://airflow-webserver:8080
      AIRFLOW_DOCTOR_WATCH: api                                     # poll the REST API; no plugin needed
    volumes:
      - doctor-data:/data

volumes:
  doctor-data:
```

The URL is your Airflow API server as seen from inside the compose network (service
name and container port, not `localhost`).

## 4. Check the setup

```bash
docker compose run --rm airflow-doctor check
```

Every line should be `✓` or `–` (optional, not configured). `check` also reads the log
of a recent failed task, which catches a broken Airflow logging setup early. Anything marked `✗` comes
with the setting to change. `check` spends nothing: the LLM key is verified with the
free token-counting endpoint. Add `--send-test` to post a test message to Slack / email.

## 5. Start it

```bash
docker compose up -d airflow-doctor
docker compose logs -f airflow-doctor
```

From now on, every task that fails shows up in the log within about 30 seconds
(`AIRFLOW_DOCTOR_POLL_INTERVAL_S`), then a diagnosis and a card. On the very first
start Doctor also looks back 60 minutes (`AIRFLOW_DOCTOR_POLL_LOOKBACK_MINUTES`).

To try it right away, make any task fail (or trigger a DAG you know fails) and watch
the log. Saved results are in the `doctor-data` volume:

```bash
docker compose exec airflow-doctor ls /data/contexts /data/diagnoses
```

## Optional

| Want | Do |
|---|---|
| Git history in diagnoses ("this broke after commit abc123") | Mount a clone of your DAG repo, e.g. `- ./:/repo:ro`, and set `AIRFLOW_DOCTOR_GIT_REPO_PATH=/repo` and `AIRFLOW_DOCTOR_GIT_DAGS_SUBDIR=dags` |
| Only some DAGs | `AIRFLOW_DOCTOR_WATCH_DAG_IDS=["sales_daily","finance_close"]` |
| Email instead of / as well as Slack | `AIRFLOW_DOCTOR_SMTP_HOST`, `_SMTP_PORT`, `_SMTP_USERNAME`, `_SMTP_PASSWORD`, `_EMAIL_FROM`, `_EMAIL_TO=["oncall@example.com"]` |
| Instant detection (seconds, not 30 s) | Install the plugin into your Airflow image (`pip install airflow-doctor`), share a folder between Airflow and the worker at `AIRFLOW_DOCTOR_HOME`, and set `AIRFLOW_DOCTOR_WATCH=both` |
| No diagnosis, just collect + notify | `AIRFLOW_DOCTOR_DIAGNOSE=false` |

## Kubernetes

Run the same image as a one-replica Deployment with the settings in a Secret and a
small PersistentVolumeClaim at `/data`; `AIRFLOW_DOCTOR_AIRFLOW_URL` is the Airflow API
server's Service, e.g. `http://airflow-api-server.airflow.svc:8080`. Polling mode needs
nothing from the Airflow pods. (A Helm chart is planned.)

## Managed Airflow

Polling only needs the REST API, so MWAA, Cloud Composer and Astronomer can work, but
each uses its own login (AWS IAM, Google OAuth, Astronomer API tokens). Airflow Doctor
currently supports a username and password, or a fixed bearer token
(`AIRFLOW_DOCTOR_AIRFLOW_TOKEN`). **Managed platforms are not verified yet.**

## Troubleshooting

| `check` says | Fix |
|---|---|
| ✗ Airflow API … not found | `AIRFLOW_DOCTOR_AIRFLOW_URL` must reach the API server from the Doctor container (service name, container port) |
| ✗ Airflow login … HTTP 401 / 403 | Wrong user or password, or the user lacks the Viewer role. Airflow 2: enable the basic auth backend |
| ✗ LLM … no Anthropic credentials | Set `AIRFLOW_DOCTOR_LLM_API_KEY`, or `AIRFLOW_DOCTOR_DIAGNOSE=false` |
| ✗ Finding failures … plugin not installed | You set `WATCH=plugin`; use `api` (default for this guide) or install the plugin |
| ! Task logs … Airflow could not serve the log | Your Airflow cannot show task logs (its own UI cannot either). Most often its components do not share `[api] secret_key`: set `AIRFLOW__API__SECRET_KEY` to the same value on every Airflow container, or share the logs folder, or use remote logging. Until then cards say "its log could not be read". |

## Costs and privacy

- A diagnosis costs about **$0.03** with Claude Opus 5.5. Set a spending limit in the
  Anthropic console.
- Logs and code stay in your network. Only the **redacted** failure context is sent to
  the model, and cards contain only redacted text and short quotes.
