# Changelog

## 0.1.0 (2026-11-25)

First release.

- **Finds failed tasks** by polling the Airflow REST API every 15 seconds (no plugin, no shared folder; works with Kubernetes and Celery workers) and, optionally, instantly through an Airflow listener plugin. Airflow 2.x (REST API v1) and 3.x (v2), tested on 2.11 and 3.3.
- **Collects** the exception and failing line, task log, DAG source, task inputs, run history and recent git commits to the DAG file. Detects when Airflow itself cannot serve a task log and says why.
- **Redacts** secrets before anything leaves the process: passwords in connection strings, `password=` / `token=` / `api_key=` values, known token formats, emails, high-entropy keys, sensitive task params and git author names. Same secret, same placeholder; one-way. Flags such as MySQL's `(using password: YES)` are kept.
- **Diagnoses** with Claude (Opus 5.5 by default) through a provider interface: summary, category, root cause, evidence quotes checked against the logs and code, confidence (capped when the evidence does not check out), suggested fix and an optional patch. About USD 0.04 to 0.06 per diagnosis.
- **Notifies** through Slack (incoming webhook) and email (SMTP), redacted again before sending; undeliverable messages go to an outbox. Cards put what to do first, then why it failed and the evidence; confidence is in words (High, Medium, Low); plain text, no emoji. Cards for mapped tasks name the map index. Webhook URLs never appear in the logs.
- **Repeats and cost**: failures are fingerprinted; the same failure within 24 h reuses its diagnosis (no model call) and gets a "still failing (N times)" card at most hourly; at most 20 diagnoses per hour.
- **`dagnose check`** verifies the whole setup, with a fix for each problem, and warns when the Airflow user can do more than read; **`dagnose worker`** runs the pipeline; **`dagnose queue`** shows job counts and the most recent jobs (`--dag`, `--last`). **`dagnose review`** grades diagnoses, confirms each grade before saving, and `--redo DAG_ID` grades one again. Docker image for Intel and ARM.
