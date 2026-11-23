"""Collect failure context through the Airflow REST API (v2 for 3.x, v1 for 2.x) and git."""

from dagnose.collector.airflow_api import AirflowAPIError, AirflowClient
from dagnose.collector.collect import collect

__all__ = ["AirflowAPIError", "AirflowClient", "collect"]
