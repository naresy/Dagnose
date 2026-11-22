"""Collect failure context through the Airflow REST API (v2 for 3.x, v1 for 2.x) and git."""

from airflow_doctor.collector.airflow_api import AirflowAPIError, AirflowClient
from airflow_doctor.collector.collect import collect

__all__ = ["AirflowAPIError", "AirflowClient", "collect"]
