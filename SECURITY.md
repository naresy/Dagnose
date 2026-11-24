# Security

Dagnose handles failure context from your Airflow (logs, DAG code, task inputs) and is
built to keep secrets out of everything it sends or stores: see "Redact" in the README.

## Reporting a vulnerability

Please **do not open a public issue**. Report it privately through GitHub:
**Security, then Report a vulnerability** on this repository (a private security advisory).
Include what you found, how to reproduce it, and what an attacker could do with it.

Especially welcome: any way a secret from a log, DAG, task parameter or connection
string can end up in a model request, a Slack or email card, or a saved file.

## Supported versions

Until 1.0, security fixes go into the latest release only.
