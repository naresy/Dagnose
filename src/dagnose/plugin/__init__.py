"""Airflow plugin: a listener that only enqueues failure jobs. Never raises.

Registered through the `airflow.plugins` entry point, so installing the package into
the Airflow environment is enough.
"""

from airflow.plugins_manager import AirflowPlugin

from dagnose.plugin import listener


class DagnosePlugin(AirflowPlugin):
    name = "dagnose"
    listeners = [listener]
