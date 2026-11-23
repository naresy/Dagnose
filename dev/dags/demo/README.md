# Demo DAGs (deliberately broken)

These DAGs fail on purpose, so Dagnose has real failures to collect, redact and
diagnose. They are written like ordinary pipelines: names, comments, tags and
docstrings say nothing about what is wrong, because the model reads the DAG source
and must not be handed the answer.

What each one should produce (exception, failing line, acceptable diagnosis
categories, and secrets that must never leak) is in [expected.json](expected.json),
which the unit tests, `dev/e2e.py` and `dev/diagnose_fixtures.py` all use.

| DAG | What is wrong |
|---|---|
| `demo_orders_tax` | `compute_tax` reads `order["country"]`, which `extract_orders` never returns |
| `demo_daily_revenue` | queries an `orders` table that no pipeline has created |
| `demo_partner_export` | ~5 minutes of work under a 10 second `execution_timeout` |
| `demo_crm_push` | connection id typo: `salesforce_prd` |
| `demo_customer_scoring` | imports a package that is not installed |
| `demo_crm_session_sync` | leaks credentials into logs, task params and the error, then fails |

**Every credential in `crm_session_sync.py` is fake**, made up to test the redactor. None of them works anywhere.
