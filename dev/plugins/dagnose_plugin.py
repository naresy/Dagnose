# Dev only: loads the Dagnose plugin from the mounted repo (see PYTHONPATH in
# docker-compose.yml). A pip-installed dagnose registers itself through the
# `airflow.plugins` entry point and does not need this file.
from dagnose.plugin import DagnosePlugin  # noqa: F401
