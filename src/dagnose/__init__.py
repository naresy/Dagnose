"""Dagnose: open source AI on-call engineer for Apache Airflow."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dagnose")  # single source: [project].version in pyproject.toml
except PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0.0.0+unknown"
