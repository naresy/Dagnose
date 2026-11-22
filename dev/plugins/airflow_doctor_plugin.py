# Dev only: loads the Airflow Doctor plugin from the mounted repo (see PYTHONPATH in
# docker-compose.yml). A pip-installed airflow-doctor registers itself through the
# `airflow.plugins` entry point and does not need this file.
from airflow_doctor.plugin import AirflowDoctorPlugin  # noqa: F401
