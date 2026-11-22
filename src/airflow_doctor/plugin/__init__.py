"""Airflow plugin: a listener that only enqueues failure jobs. Never raises.

Registered through the `airflow.plugins` entry point, so installing the package into
the Airflow environment is enough.
"""

from airflow.plugins_manager import AirflowPlugin

from airflow_doctor.plugin import listener


class AirflowDoctorPlugin(AirflowPlugin):
    name = "airflow_doctor"
    listeners = [listener]
