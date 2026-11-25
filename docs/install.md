# Install Dagnose in your Airflow project

Dagnose runs **next to** your Airflow as one extra container. In the default
setup it only needs a read-only Airflow login: no plugin, no changes to your DAGs or
your Airflow image, no shared folder. It finds failed tasks by polling Airflow's REST
API, then collects, redacts, diagnoses and sends a card to Slack or email.

Tested with **Airflow 3.3** and **Airflow 2.11** (Docker Compose, LocalExecutor).
Time: about 10 minutes.

## 1. Create a read-only Airflow user

Dagnose only reads: DAGs, DAG runs, task instances and logs. The built-in **Viewer**
role is enough. Do not use your own login or the admin account: `check` (step 4) warns
when the user can do more than read.

**Airflow 3 with the FAB auth manager, or Airflow 2** (in your Airflow container):

```bash
docker compose exec airflow-apiserver airflow users create \
  --role Viewer --username dagnose --password 'choose-a-long-password' \
  --firstname Dagnose --lastname Bot --email dagnose@example.com
```

On Airflow 2 the container is usually `airflow-webserver`, and the REST API must accept
basic auth: `AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session`.

**Airflow 3 with the default simple auth manager:** add `dagnose:viewer` to
`[core] simple_auth_manager_users` (`AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`); the
password is written to the generated passwords file.

## 2. Settings: `dagnose.env`

In the folder with your `docker-compose.yml`, create a **new** file `dagnose.env`. If
the folder already has a `.env` (the official Airflow setup puts `AIRFLOW_UID` there),
leave that file as it is: Airflow needs it, and Dagnose's settings go in their own file.

```bash
DAGNOSE_AIRFLOW_USERNAME=dagnose
DAGNOSE_AIRFLOW_PASSWORD=choose-a-long-password
DAGNOSE_LLM_API_KEY=sk-ant-...            # Anthropic key for the diagnosis
# No key yet? Use DAGNOSE_DIAGNOSE=false instead: failures are still collected,
# redacted and sent, and cards say "Not diagnosed".
DAGNOSE_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...   # optional
```

Write each line as `NAME=value`: no spaces around `=`, no quotes, no `export`. Then make
the file readable only by you (it holds the API key) and keep it out of git:

```bash
chmod 600 dagnose.env && echo dagnose.env >> .gitignore
```

## 3. Add the worker to your `docker-compose.yml`

Two additions, in two different places of the file:

1. The `dagnose` service goes **inside** `services:`, indented like your other services
   (for example after the last one).
2. `dagnose-data:` goes in the **top-level** `volumes:` at the end of the file, next to
   the volumes that are already there (the official Airflow file has
   `postgres-db-volume:`). It is not part of the service.

```yaml
services:
  # ... your Airflow services, unchanged ...

  dagnose:
    image: ghcr.io/naresy/dagnose:latest
    restart: unless-stopped
    env_file: dagnose.env
    environment:
      DAGNOSE_AIRFLOW_URL: http://airflow-apiserver:8080   # Airflow 2: http://airflow-webserver:8080
      DAGNOSE_WATCH: api                                   # poll the REST API; no plugin needed
      DAGNOSE_AIRFLOW_UI_URL: http://localhost:8080        # what you type in the browser
    volumes:
      - dagnose-data:/data

volumes:
  # ... your existing volumes, unchanged ...
  dagnose-data:
```

- `DAGNOSE_AIRFLOW_URL` is your Airflow API server as seen from **inside** the compose
  network: service name and container port, not `localhost`.
- `DAGNOSE_AIRFLOW_UI_URL` is the address you open Airflow with in your browser. The
  "Open in Airflow" buttons on cards use it; without it they point at the internal name,
  which a browser cannot open.
- Check the file before going on: `docker compose config --quiet` prints nothing when it
  is valid.

> **Before the first release** the image is not on ghcr.io yet. Build it from the
> source instead, and use `image: dagnose:local`:
> `git clone https://github.com/naresy/dagnose && docker build -t dagnose:local dagnose`

## 4. Check the setup

```bash
docker compose run --rm dagnose check
```

Every line should be `[ok]` or `[skip]` (optional, not configured). `check` also reads the log
of a recent failed task, which catches a broken Airflow logging setup early. Anything marked `[fail]` comes
with the setting to change. `check` spends nothing: the LLM key is verified with the
free token-counting endpoint. Add `--send-test` to post a test message to Slack / email.

## 5. Start it

```bash
docker compose up -d dagnose
docker compose logs -f dagnose
```

From now on, every task that fails shows up in the log within about 15 seconds
(`DAGNOSE_POLL_INTERVAL_S`), then a diagnosis and a card. On the very first
start Dagnose also looks back 60 minutes (`DAGNOSE_POLL_LOOKBACK_MINUTES`).

To try it right away, make any task fail (or trigger a DAG you know fails) and watch
the log. In the official Airflow setup new DAGs start **paused**
(`DAGS_ARE_PAUSED_AT_CREATION`): switch yours on in the UI before triggering it. Saved
results are in the `dagnose-data` volume:

```bash
docker compose exec dagnose ls /data/contexts /data/diagnoses
```

## Reading a card

- **Headline:** the category (code bug, configuration, permissions, dependency,
  upstream data, timeout, infrastructure) and a **confidence** from 0 to 1. Above about
  0.85 it is usually right; below about 0.7, read the root cause critically.
- **Evidence:** quotes from the log, the DAG source, the task's inputs or the run
  history. A check mark means the quote was found word for word; a question mark means
  it was not, and counts for less.
- **Suggested fix:** the smallest change that fixes it. Often it is not a code change
  ("update the connection password", "the database is down"). "A patch is in the
  diagnosis file" means a ready diff is in `/data/diagnoses/`.
- **Secrets** appear as `[REDACTED:password#1]`. That is on purpose.
- **Repeats:** the same failure again within 24 hours gets no new model call and at
  most one "still failing (N times)" card per hour.

## Weekly upkeep

| Command (prefix with `docker compose exec dagnose`) | Look for |
|---|---|
| `dagnose queue` | `error 0`, nothing stuck in `pending`; the latest jobs by name |
| `dagnose outbox` | `0 waiting`; if not (chat was down, webhook changed): `dagnose outbox --resend` |
| `dagnose review` (add `-it` after `exec`) | grade new diagnoses, to know how far to trust them |
| `dagnose review --report` | accuracy, cost and speed so far |

**Upgrade:** `docker compose pull dagnose && docker compose up -d dagnose` (data is kept).

**Remove:** delete the `dagnose` service and the `dagnose-data` volume line from
`docker-compose.yml`, run `docker compose down` and `docker volume rm <project>_dagnose-data`,
delete the `dagnose` user in Airflow, and remove the webhook in your chat tool.

## Optional

| Want | Do |
|---|---|
| Git history in diagnoses ("this broke after commit abc123") | Mount a clone of your DAG repo, e.g. `- ./:/repo:ro`, and set `DAGNOSE_GIT_REPO_PATH=/repo` and `DAGNOSE_GIT_DAGS_SUBDIR=dags` |
| Only some DAGs | `DAGNOSE_WATCH_DAG_IDS=["sales_daily","finance_close"]` |
| Microsoft Teams | In the channel, Workflows, then "Post to a channel when a webhook request is received"; set `DAGNOSE_TEAMS_WEBHOOK_URL` to its URL |
| Google Chat | In the space, Apps & integrations, then Webhooks; set `DAGNOSE_GOOGLE_CHAT_WEBHOOK_URL` |
| Email instead of / as well as Slack | `DAGNOSE_SMTP_HOST`, `_SMTP_PORT`, `_SMTP_USERNAME`, `_SMTP_PASSWORD`, `_EMAIL_FROM`, `_EMAIL_TO=["oncall@example.com"]` |
| Instant detection (seconds, not 15 s) | Install the plugin into your Airflow image (`pip install dagnose`), share a folder between Airflow and the worker at `DAGNOSE_HOME`, and set `DAGNOSE_WATCH=both` |
| No diagnosis, just collect + notify | `DAGNOSE_DIAGNOSE=false` |

## Kubernetes, ECS, Cloud Run, Container Apps

See [deploy-cloud.md](deploy-cloud.md): a ready Kubernetes manifest (EKS, GKE, AKS), an
ECS Fargate task definition, and notes for Cloud Run and Azure Container Apps. On
Kubernetes, `DAGNOSE_AIRFLOW_URL` is the Airflow API server's Service, e.g.
`http://airflow-api-server.airflow.svc:8080`.

## Managed Airflow (AWS, Google Cloud, Azure)

Polling only needs the Airflow REST API, so Dagnose works with managed Airflow; what
changes is how it logs in. The Docker image includes the AWS, Google and Azure SDKs
(`pip install 'dagnose[aws]'`, `[gcp]` or `[azure]` otherwise).

> **Status:** built and tested offline (the MWAA requests are checked against the AWS
> SDK's own service model; the full collector runs over a simulated MWAA). A first
> run against a real environment on each cloud is still to do.

### AWS: Amazon MWAA (Airflow 2.4.3+)

Dagnose calls the Airflow REST API through MWAA's `InvokeRestApi`, signed with the AWS
credentials it runs with (ECS task role, EC2 instance profile, or a profile). Give that
principal read-only access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": "airflow:InvokeRestApi",
     "Resource": "arn:aws:airflow:REGION:ACCOUNT_ID:role/ENVIRONMENT_NAME/Viewer"},
    {"Effect": "Allow", "Action": "airflow:GetEnvironment",
     "Resource": "arn:aws:airflow:REGION:ACCOUNT_ID:environment/ENVIRONMENT_NAME"}
  ]
}
```

```bash
DAGNOSE_AIRFLOW_AUTH=mwaa
DAGNOSE_MWAA_ENVIRONMENT=ENVIRONMENT_NAME
DAGNOSE_AWS_REGION=eu-west-1
DAGNOSE_AIRFLOW_API=v1        # optional: Airflow 2 = v1, 3 = v2; then GetEnvironment is not needed
```

- With a **private** webserver, run Dagnose inside the same VPC (ECS Fargate is simplest).
- Turn on **task logs** in the environment's logging settings; Dagnose reads them through
  the API. MWAA caps one API response at 6 MB.

### Google Cloud: Cloud Composer

Dagnose uses Google default credentials (a service account; on GKE, Workload Identity;
on Cloud Run, the service's account) and sends them as a bearer token.

1. Give the service account `roles/composer.user`.
2. Make it a **Viewer** in Airflow. Composer registers new API users as **Op** by default,
   which can change things: set the Airflow config override
   `api` / `composer_auth_user_registration_role` = `Viewer` before Dagnose's first call.
3. Settings:

```bash
DAGNOSE_AIRFLOW_AUTH=google
DAGNOSE_AIRFLOW_URL=$(gcloud composer environments describe ENV --location REGION --format="value(config.airflowUri)")
```

### Azure

- **Airflow on AKS or VMs** (the usual setup): the main guide above, nothing extra.
- **Apache Airflow jobs in Microsoft Fabric** (Azure Data Factory's Workflow Orchestration
  Manager was retired in favour of it): Dagnose can send a Microsoft Entra ID token from
  `DefaultAzureCredential` (managed identity, workload identity, or `az login`). Set the
  Airflow URL and the API's Entra scope. *Experimental: the scope to use is not verified yet.*

```bash
DAGNOSE_AIRFLOW_AUTH=azure
DAGNOSE_AIRFLOW_URL=https://...
DAGNOSE_AZURE_SCOPE=<application-id-uri>/.default
```

### The model inside your own cloud

By default Dagnose calls the Claude API. To keep even the (redacted) prompts inside your
cloud account, run Claude on your cloud's AI platform instead. The diagnosis is the same;
only the automatic fallback model is unavailable there.

| Cloud | Settings | Access needed |
|---|---|---|
| AWS: Amazon Bedrock | `DAGNOSE_LLM_PROVIDER=bedrock`, `DAGNOSE_LLM_REGION=us-east-1` (or `DAGNOSE_AWS_REGION`) | Model access to Claude Opus 5.5 in Bedrock; the AWS credentials Dagnose runs with |
| Google Cloud: Vertex AI | `DAGNOSE_LLM_PROVIDER=vertex`, `DAGNOSE_LLM_PROJECT=my-project` (region `global` by default) | Claude enabled in Vertex AI Model Garden; `roles/aiplatform.user` |
| Azure: Microsoft Foundry | `DAGNOSE_LLM_PROVIDER=foundry`, `DAGNOSE_LLM_FOUNDRY_RESOURCE=my-resource`, and `DAGNOSE_LLM_API_KEY`, or no key to use Entra ID | A Claude deployment in your Foundry resource |

`dagnose check` verifies the model access for free (token counting) on all of them.

### Astronomer

Use a Deployment API token as `DAGNOSE_AIRFLOW_TOKEN` with the deployment's URL
(`DAGNOSE_AIRFLOW_AUTH` stays `password`). Not verified yet.

## Troubleshooting

| `check` says | Fix |
|---|---|
| [fail] Airflow API ... not found | `DAGNOSE_AIRFLOW_URL` must reach the API server from the Dagnose container (service name, container port) |
| [fail] Airflow login ... HTTP 401 / 403 | Wrong user or password, or the user lacks the Viewer role. Airflow 2: enable the basic auth backend |
| [warn] Airflow login ... more than read-only | The user is an Op or Admin. Create a Viewer user (step 1) and put it in `dagnose.env` |
| [fail] LLM ... no Anthropic credentials | Set `DAGNOSE_LLM_API_KEY`, or `DAGNOSE_DIAGNOSE=false` |
| [fail] Airflow API ... not installed: pip install 'dagnose[aws]' | Install the extra for your cloud, or use the Docker image (it has all of them) |
| [fail] Airflow API ... MWAA AccessDeniedException / HTTP 403 | The IAM principal lacks `airflow:InvokeRestApi` on `role/ENV/Viewer` (see MWAA above) |
| [fail] Finding failures ... plugin not installed | You set `WATCH=plugin`; use `api` (default for this guide) or install the plugin |
| [warn] Task logs ... Airflow could not serve the log | Your Airflow cannot show task logs (its own UI cannot either). Most often its components do not share `[api] secret_key`: set `AIRFLOW__API__SECRET_KEY` to the same value on every Airflow container, or share the logs folder, or use remote logging. Until then cards say "its log could not be read". |

## Costs and privacy

- A diagnosis costs about **USD 0.04** with Claude Opus 5.5 (measured: USD 0.039 on
  average). A failure that repeats reuses
  its diagnosis for 24 hours (`DAGNOSE_DEDUPE_WINDOW_HOURS`) and gets a "still failing"
  card at most hourly (`DAGNOSE_RENOTIFY_MINUTES`); at most 20 diagnoses per hour
  (`DAGNOSE_MAX_DIAGNOSES_PER_HOUR`). Also set a spending limit with your model provider.
- Logs and code stay in your network. Only the **redacted** failure context is sent to
  the model, and cards contain only redacted text and short quotes.
